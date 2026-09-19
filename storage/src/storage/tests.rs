//! Tests storage lane admission, shutdown fences, transaction cleanup, reads and metrics.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{FjallPersistPolicy, FjallStorage, StorageHandle};
use aruna_core::effects::{Effect, IterStart, StorageEffect, StoragePriority};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::BLOB_CLEANUP_KEYSPACE;
use std::future::{Future, poll_fn};
use std::sync::atomic::Ordering;
use std::task::Poll;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use ulid::Ulid;

fn small_handle(capacity: usize) -> (StorageHandle, super::StorageReceivers) {
    let (sender, foreground) = crossfire::mpsc::bounded_blocking(capacity);
    let (bulk_sender, bulk) = crossfire::mpsc::bounded_blocking(capacity);
    let handle = StorageHandle {
        write_async: sender.clone().into_async(),
        bulk_async: bulk_sender.clone().into_async(),
        write_channel: sender,
        bulk_channel: bulk_sender,
        priority: StoragePriority::Foreground,
        metrics: std::sync::Arc::new(super::StorageMetrics::default()),
        transaction_cleanup: std::sync::Arc::new(std::sync::Mutex::new(
            std::collections::BTreeMap::new(),
        )),
        worker: std::sync::Arc::new(std::sync::Mutex::new(None)),
    };
    (handle, super::StorageReceivers { foreground, bulk })
}

fn cleanup_write() -> StorageEffect {
    StorageEffect::Write {
        key_space: BLOB_CLEANUP_KEYSPACE.to_string(),
        key: b"cleanup".to_vec().into(),
        value: b"work".to_vec().into(),
        txn_id: None,
    }
}

#[test]
fn foreground_precedes_bulk() {
    // All four effects are enqueued before the first recv, so lane order is
    // deterministic: the single foreground effect precedes the three bulk.
    let (handle, receivers) = StorageHandle::new();
    let read = |key_space: &str| StorageEffect::Read {
        key_space: key_space.to_string(),
        key: b"k".to_vec().into(),
        txn_id: None,
    };
    let mut keep = Vec::new();
    for key_space in ["b1", "b2", "b3"] {
        let effect = read(key_space);
        let (tx, rx) = super::response_channel(super::ResponseToken::empty());
        let span = super::storage_effect_span(&effect);
        let in_flight = super::InFlightGuard::acquire(&handle.metrics);
        assert!(
            handle
                .bulk_channel
                .try_send((effect, tx, span, Instant::now(), in_flight))
                .is_ok(),
            "bulk enqueue"
        );
        keep.push(rx);
    }
    let effect = read("fg");
    let (tx, rx) = super::response_channel(super::ResponseToken::empty());
    let span = super::storage_effect_span(&effect);
    let in_flight = super::InFlightGuard::acquire(&handle.metrics);
    assert!(
        handle
            .write_channel
            .try_send((effect, tx, span, Instant::now(), in_flight))
            .is_ok(),
        "foreground enqueue"
    );
    keep.push(rx);

    let mut lanes = super::LaneScheduler::default();
    let (first, priority) = lanes.next(&receivers).expect("first item");
    assert_eq!(priority, StoragePriority::Foreground);
    assert_eq!(super::effect_keyspace(&first.0), Some("fg"));
    for _ in 0..3 {
        let (_, priority) = lanes.next(&receivers).expect("bulk item");
        assert_eq!(priority, StoragePriority::Bulk);
    }
    drop(keep);
}

#[test]
fn bulk_keeps_share() {
    // Credit each foreground effect so a large batch cannot starve the bulk drain.
    let (handle, receivers) = StorageHandle::new();
    let read = |key_space: &str| StorageEffect::Read {
        key_space: key_space.to_string(),
        key: b"k".to_vec().into(),
        txn_id: None,
    };
    let mut keep = Vec::new();
    let mut enqueue = |channel: &super::EffectSender, key_space: &str| {
        let effect = read(key_space);
        let (tx, rx) = super::response_channel(super::ResponseToken::empty());
        let span = super::storage_effect_span(&effect);
        let in_flight = super::InFlightGuard::acquire(&handle.metrics);
        assert!(
            channel
                .try_send((effect, tx, span, Instant::now(), in_flight))
                .is_ok(),
            "enqueue {key_space}"
        );
        keep.push(rx);
    };
    let batch = super::MAX_GROUP_COMMIT;
    let expected_bulk = batch / super::FOREGROUND_PER_BULK;
    for _ in 0..batch {
        enqueue(&handle.write_channel, "fg");
    }
    for _ in 0..expected_bulk {
        enqueue(&handle.bulk_channel, "bulk");
    }

    let mut lanes = super::LaneScheduler::default();
    let (_, priority) = lanes.next(&receivers).expect("first item");
    assert_eq!(priority, StoragePriority::Foreground);
    // The actor drains the rest of the foreground queue as one batch.
    lanes.record_foreground(batch);

    let mut bulk_served = 0usize;
    for _ in 0..expected_bulk {
        let (_, priority) = lanes.next(&receivers).expect("item");
        if priority == StoragePriority::Bulk {
            bulk_served += 1;
        }
    }
    assert_eq!(
        bulk_served, expected_bulk,
        "a full foreground batch buys {expected_bulk} bulk slots"
    );
    drop(keep);
}

#[test]
fn abort_routes_foreground() {
    // A saturated bulk lane must not swallow a bulk handle's abort; aborts
    // free resources and always dispatch on the foreground lane.
    let (handle, receivers) = StorageHandle::new();
    let mut keep = Vec::new();
    loop {
        let effect = StorageEffect::Read {
            key_space: "b".to_string(),
            key: b"k".to_vec().into(),
            txn_id: None,
        };
        let (tx, rx) = super::response_channel(super::ResponseToken::empty());
        let span = super::storage_effect_span(&effect);
        let in_flight = super::InFlightGuard::acquire(&handle.metrics);
        if handle
            .bulk_channel
            .try_send((effect, tx, span, Instant::now(), in_flight))
            .is_err()
        {
            break;
        }
        keep.push(rx);
    }

    let txn_id = Ulid::from_parts(1, 1);
    handle.bulk().enqueue_abort_transaction(txn_id, "test");

    let (effect, ..) = receivers
        .foreground
        .try_recv()
        .expect("abort routed to foreground");
    assert!(matches!(
        effect,
        StorageEffect::AbortTransaction { txn_id: got } if got == txn_id
    ));
    assert!(
        receivers.bulk.try_recv().is_ok(),
        "bulk lane still saturated"
    );
    drop(keep);
}

#[tokio::test]
async fn cleanup_waits_space() {
    let (handle, receivers) = small_handle(1);
    let filler = StorageEffect::Read {
        key_space: "ordinary".to_string(),
        key: b"filler".to_vec().into(),
        txn_id: None,
    };
    let (filler_tx, _filler_rx) = super::response_channel(super::ResponseToken::empty());
    let filler_span = super::storage_effect_span(&filler);
    let filler_guard = super::InFlightGuard::acquire(&handle.metrics);
    handle
        .write_channel
        .try_send((filler, filler_tx, filler_span, Instant::now(), filler_guard))
        .expect("fill queue");

    let receiver = receivers.foreground.into_async();
    let waiter = tokio::spawn({
        let handle = handle.clone();
        async move { handle.send_storage_effect(cleanup_write()).await }
    });
    let (effect, ..) = receiver.recv().await.expect("filler effect");
    assert!(!super::is_cleanup_write(&effect));
    assert!(handle.transaction_cleanup.try_lock().is_ok());
    let (effect, response_tx, ..) = receiver.recv().await.expect("cleanup effect");
    assert!(super::is_cleanup_write(&effect));
    assert!(response_tx.send(StorageEvent::WriteResult {
        key: b"cleanup".to_vec().into(),
    }));

    assert!(matches!(
        waiter.await.expect("cleanup sender"),
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}

#[tokio::test]
async fn ordinary_write_full() {
    let (handle, receivers) = small_handle(1);
    let filler = StorageEffect::Read {
        key_space: "ordinary".to_string(),
        key: b"filler".to_vec().into(),
        txn_id: None,
    };
    let (filler_tx, _filler_rx) = super::response_channel(super::ResponseToken::empty());
    let filler_span = super::storage_effect_span(&filler);
    let filler_guard = super::InFlightGuard::acquire(&handle.metrics);
    handle
        .write_channel
        .try_send((filler, filler_tx, filler_span, Instant::now(), filler_guard))
        .expect("fill queue");

    let event = handle
        .send_storage_effect(StorageEffect::Write {
            key_space: "ordinary".to_string(),
            key: b"target".to_vec().into(),
            value: b"value".to_vec().into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::Error {
            error: StorageError::QueueFull
        })
    ));
    drop(receivers);
}

#[tokio::test]
async fn closed_cleanup_stops() {
    let (handle, receivers) = small_handle(1);
    drop(receivers.foreground);

    let event = handle.send_storage_effect(cleanup_write()).await;

    assert!(matches!(
        event,
        Event::Storage(StorageEvent::Error {
            error: StorageError::ChannelClosed
        })
    ));
}

#[tokio::test]
async fn bulk_lane_works() {
    let dir = tempdir().unwrap();
    let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let bulk = handle.bulk();
    let write = bulk
        .send_storage_effect(StorageEffect::Write {
            key_space: "node_state".to_string(),
            key: b"k".to_vec().into(),
            value: b"v".to_vec().into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        write,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
    let read = bulk
        .send_storage_effect(StorageEffect::Read {
            key_space: "node_state".to_string(),
            key: b"k".to_vec().into(),
            txn_id: None,
        })
        .await;
    match read {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => assert_eq!(value.as_ref(), b"v"),
        other => panic!("unexpected storage event: {other:?}"),
    }
}

#[test]
fn bulk_full_rejects() {
    // A saturated bulk read pool rejects with QueueFull instead of running
    // the read inline on the write actor thread.
    let dir = tempdir().unwrap();
    let db = fjall::OptimisticTxDatabase::builder(dir.path())
        .manual_journal_persist(true)
        .open()
        .unwrap();
    let (bulk_sender, _bulk_receiver) = crossfire::mpsc::bounded_blocking(1);
    let mut storage = super::FjallStorage {
        store: super::Store::new(db),
        persist_policy: FjallPersistPolicy::default(),
        txns: std::collections::HashMap::new(),
        transaction_cleanup: std::sync::Arc::new(std::sync::Mutex::new(
            std::collections::BTreeMap::new(),
        )),
        metrics: std::sync::Arc::new(super::StorageMetrics::default()),
        read_pool: Vec::new(),
        next_reader: 0,
        bulk_read_pool: vec![bulk_sender],
        next_bulk_reader: 0,
        pool_threads: Vec::new(),
        compactor: crate::compaction::Compactor::idle(),
        deletes: std::collections::HashMap::new(),
        txn_deletes: std::collections::HashMap::new(),
    };
    let metrics = std::sync::Arc::new(super::StorageMetrics::default());
    let read_effect = || StorageEffect::Read {
        key_space: "node_state".to_string(),
        key: b"missing".to_vec().into(),
        txn_id: None,
    };

    let filler = read_effect();
    let span = super::storage_effect_span(&filler);
    let guard = super::InFlightGuard::acquire(&metrics);
    let (filler_tx, _filler_rx) = super::response_channel(super::ResponseToken::empty());
    assert!(
        storage.bulk_read_pool[0]
            .try_send((filler, filler_tx, span, Instant::now(), guard))
            .is_ok(),
        "saturate bulk read pool"
    );

    let target = read_effect();
    let span = super::storage_effect_span(&target);
    let guard = super::InFlightGuard::acquire(&metrics);
    let (target_tx, mut target_rx) = super::response_channel(super::ResponseToken::empty());
    let mut slow = super::SlowQueueAggregator::default();
    storage.forward_read(
        (target, target_tx, span, Instant::now(), guard),
        StoragePriority::Bulk,
        &mut slow,
    );

    match target_rx.try_recv() {
        Ok((
            StorageEvent::Error {
                error: StorageError::QueueFull,
            },
            _,
        )) => {}
        other => panic!("expected QueueFull rejection, got {other:?}"),
    }
}

pub(super) fn assert_write_result(event: Event, expected_key: &[u8]) {
    match event {
        Event::Storage(StorageEvent::WriteResult { key }) => {
            assert_eq!(key.as_ref(), expected_key);
        }
        other => panic!("unexpected storage event: {other:?}"),
    }
}

fn assert_batch_write(event: Event, expected: &[(&str, &[u8])]) {
    match event {
        Event::Storage(StorageEvent::BatchWriteResult { entries }) => {
            let actual = entries
                .iter()
                .map(|(key_space, key)| (key_space.as_str(), key.as_ref()))
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
        }
        other => panic!("unexpected storage event: {other:?}"),
    }
}

fn assert_batch_delete(event: Event, expected: &[(&str, &[u8])]) {
    match event {
        Event::Storage(StorageEvent::BatchDeleteResult { entries }) => {
            let actual = entries
                .iter()
                .map(|(key_space, key)| (key_space.as_str(), key.as_ref()))
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
        }
        other => panic!("unexpected storage event: {other:?}"),
    }
}

pub(super) fn assert_read_result(event: Event, expected_key: &[u8], expected_value: &[u8]) {
    match event {
        Event::Storage(StorageEvent::ReadResult {
            key,
            value: Some(value),
        }) => {
            assert_eq!(key.as_ref(), expected_key);
            assert_eq!(value.as_ref(), expected_value);
        }
        other => panic!("unexpected storage event: {other:?}"),
    }
}

#[test]
fn policy_defaults_buffer() {
    assert_eq!(FjallPersistPolicy::default(), FjallPersistPolicy::Buffer);
    assert_eq!(FjallPersistPolicy::default().label(), "buffer");
}

#[test]
fn policy_accepts_values() {
    assert_eq!(
        "sync_all".parse::<FjallPersistPolicy>().unwrap(),
        FjallPersistPolicy::SyncAll
    );
    assert_eq!(
        "buffer".parse::<FjallPersistPolicy>().unwrap(),
        FjallPersistPolicy::Buffer
    );
}

#[test]
fn policy_rejects_invalid() {
    assert!("always".parse::<FjallPersistPolicy>().is_err());
    assert!("sync".parse::<FjallPersistPolicy>().is_err());
    assert!("sync-all".parse::<FjallPersistPolicy>().is_err());
    assert!("syncall".parse::<FjallPersistPolicy>().is_err());
    assert!("buffered".parse::<FjallPersistPolicy>().is_err());
}

#[tokio::test]
async fn open_accepts_sync() {
    let dir = tempdir().expect("temp dir");
    let handle = FjallStorage::open_with_policy(
        dir.path().to_str().expect("utf-8 path"),
        FjallPersistPolicy::SyncAll,
    )
    .expect("storage opens");

    assert_write_result(
        handle
            .send_effect(Effect::Storage(StorageEffect::Write {
                key_space: "persist_policy".to_string(),
                key: b"key".to_vec().into(),
                value: b"value".to_vec().into(),
                txn_id: None,
            }))
            .await,
        b"key",
    );
}

#[tokio::test]
async fn sync_effect_succeeds() {
    let dir = tempdir().unwrap();
    let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();

    let event = handle.send_storage_effect(StorageEffect::SyncAll).await;

    assert!(matches!(
        event,
        Event::Storage(StorageEvent::SyncAllFinished)
    ));
}

#[tokio::test]
async fn cancelled_stays_counted() {
    let (handle, receivers) = StorageHandle::new();
    let receiver = receivers.foreground;
    let mut probe = Box::pin(handle.send_storage_effect(StorageEffect::Read {
        key_space: "node_state".to_string(),
        key: b"node_state".to_vec().into(),
        txn_id: None,
    }));

    // No worker consumes the effect, so the external timeout fires and the
    // probe future is dropped mid-await, as in a readiness probe timeout.
    let timed_out =
        tokio::time::timeout(std::time::Duration::from_millis(50), probe.as_mut()).await;
    assert!(timed_out.is_err());
    assert_eq!(handle.in_flight(), 1);

    drop(probe);
    assert_eq!(handle.in_flight(), 1);

    let queued = receiver.recv().expect("cancelled effect remains queued");
    assert_eq!(handle.in_flight(), 1);
    drop(queued);
    assert_eq!(handle.in_flight(), 0);
}

#[tokio::test]
async fn failed_enqueue_balances() {
    let (handle, receivers) = StorageHandle::new();
    let receiver = receivers.foreground;
    drop(receiver);

    let event = handle
        .send_storage_effect(StorageEffect::Read {
            key_space: "node_state".to_string(),
            key: b"node_state".to_vec().into(),
            txn_id: None,
        })
        .await;

    assert!(matches!(
        event,
        Event::Storage(StorageEvent::Error {
            error: StorageError::ChannelClosed
        })
    ));
    assert_eq!(handle.in_flight(), 0);
}

#[test]
fn worker_exit_latches() {
    let dir = tempdir().expect("temp dir");
    let handle =
        FjallStorage::open(dir.path().to_str().expect("utf-8 path")).expect("storage opens");
    let StorageHandle {
        write_channel,
        bulk_channel,
        write_async,
        bulk_async,
        priority: _,
        metrics,
        transaction_cleanup: _,
        worker: _,
    } = handle;

    assert!(!metrics.channel_closed.load(Ordering::Relaxed));
    drop(write_channel);
    drop(bulk_channel);
    drop(write_async);
    drop(bulk_async);

    let deadline = Instant::now() + Duration::from_secs(5);
    while !metrics.channel_closed.load(Ordering::Relaxed) && Instant::now() < deadline {
        thread::yield_now();
    }

    assert!(metrics.channel_closed.load(Ordering::Relaxed));
}

// The close is the write-after-sync barrier: a leaked child that survives the
// drain gets an error instead of committing behind the final sync.
#[tokio::test]
async fn close_rejects_writes() {
    let dir = tempdir().unwrap();
    let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    handle.close_writes();

    let event = handle
        .send_storage_effect(StorageEffect::Write {
            key_space: "closed".to_string(),
            key: b"key".to_vec().into(),
            value: b"value".to_vec().into(),
            txn_id: None,
        })
        .await;

    assert!(matches!(
        event,
        Event::Storage(StorageEvent::Error {
            error: StorageError::Closed
        })
    ));
    assert_eq!(handle.rejected_writes(), 1);
    assert!(handle.snapshot_metrics().closed);
}

// Shutdown still has to read and fsync after the barrier is up.
#[tokio::test]
async fn read_sync_allowed() {
    let dir = tempdir().unwrap();
    let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    handle
        .send_storage_effect(StorageEffect::Write {
            key_space: "closed".to_string(),
            key: b"key".to_vec().into(),
            value: b"value".to_vec().into(),
            txn_id: None,
        })
        .await;
    handle.close_writes();

    let read = handle
        .send_storage_effect(StorageEffect::Read {
            key_space: "closed".to_string(),
            key: b"key".to_vec().into(),
            txn_id: None,
        })
        .await;

    assert!(matches!(
        read,
        Event::Storage(StorageEvent::ReadResult { value: Some(_), .. })
    ));
    assert!(handle.sync_all().await.is_ok());
    assert_eq!(handle.rejected_writes(), 0);
}

// The close only blocks later dispatches, so a mutation already queued on the
// bulk lane must be waited out before the final sync.
#[tokio::test]
async fn drain_waits_accepted() {
    let (handle, receivers) = small_handle(4);
    let bulk = handle.bulk();
    let mut queued = Box::pin(bulk.send_storage_effect(StorageEffect::Write {
        key_space: "bulk".to_string(),
        key: b"key".to_vec().into(),
        value: b"value".to_vec().into(),
        txn_id: None,
    }));
    let queued_state = poll_fn(|cx| Poll::Ready(queued.as_mut().poll(cx))).await;
    assert!(queued_state.is_pending());
    assert_eq!(handle.in_flight(), 1);
    handle.close_writes();

    let accepted = receivers.bulk.recv().expect("effect stays queued");
    let mut drain = Box::pin(handle.drain_accepted(Duration::from_secs(30)));
    let drain_state = poll_fn(|cx| Poll::Ready(drain.as_mut().poll(cx))).await;
    assert!(drain_state.is_pending());
    drop(accepted);
    drop(queued);

    assert!(drain.await);
    assert_eq!(handle.in_flight(), 0);
}

#[tokio::test]
async fn write_txn_rejected() {
    let dir = tempdir().unwrap();
    let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    handle.close_writes();

    let write_txn = handle
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await;
    let read_txn = handle
        .send_storage_effect(StorageEffect::StartTransaction { read: true })
        .await;

    assert!(matches!(
        write_txn,
        Event::Storage(StorageEvent::Error {
            error: StorageError::Closed
        })
    ));
    assert!(matches!(
        read_txn,
        Event::Storage(StorageEvent::TransactionStarted { .. })
    ));
}

/// Worker sharing the handle's channels, cleanup map and fence, driven by
/// the test thread so effect order is exact.
fn worker(dir: &tempfile::TempDir, handle: &StorageHandle) -> FjallStorage {
    let db = fjall::OptimisticTxDatabase::builder(dir.path().to_str().expect("utf-8 path"))
        .manual_journal_persist(true)
        .open()
        .expect("database opens");
    FjallStorage::new(
        super::Store::new(db),
        FjallPersistPolicy::default(),
        handle,
        super::READ_POOL_THREADS,
        super::BULK_POOL_THREADS,
    )
}

fn keyed_write(key: &str) -> StorageEffect {
    StorageEffect::Write {
        key_space: "fenced".to_string(),
        key: key.as_bytes().to_vec().into(),
        value: b"value".to_vec().into(),
        txn_id: None,
    }
}

fn keyed_read(key: &str) -> StorageEffect {
    StorageEffect::Read {
        key_space: "fenced".to_string(),
        key: key.as_bytes().to_vec().into(),
        txn_id: None,
    }
}

fn serve_next(storage: &mut FjallStorage, receiver: &super::EffectReceiver) {
    let item = receiver.recv().expect("effect stays queued");
    storage.process_single(item, &mut super::SlowQueueAggregator::default());
}

/// Polls a dispatch once: the effect is enqueued and still awaiting a reply.
async fn poll_queued<F: Future>(future: &mut std::pin::Pin<Box<F>>) {
    assert!(
        poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx)))
            .await
            .is_pending(),
        "effect should stay queued"
    );
}

// The single-threaded actor finishes the mutation it already started, then
// syncs; a mutation still queued when the fence lands never starts.
#[tokio::test]
async fn active_precedes_sync() {
    let dir = tempdir().unwrap();
    let (handle, receivers) = StorageHandle::new();
    let mut storage = worker(&dir, &handle);
    let mut active = Box::pin(handle.send_storage_effect(keyed_write("active")));
    poll_queued(&mut active).await;
    let mut queued = Box::pin(handle.send_storage_effect(keyed_write("queued")));
    poll_queued(&mut queued).await;
    let mut sync = Box::pin(handle.sync_all());
    poll_queued(&mut sync).await;

    serve_next(&mut storage, &receivers.foreground);
    handle.fence_mutations();
    serve_next(&mut storage, &receivers.foreground);
    serve_next(&mut storage, &receivers.foreground);

    assert!(matches!(
        active.await,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
    assert!(matches!(
        queued.await,
        Event::Storage(StorageEvent::Error {
            error: StorageError::Closed
        })
    ));
    assert!(sync.await.is_ok());
    let mut committed = Box::pin(handle.send_storage_effect(keyed_read("active")));
    poll_queued(&mut committed).await;
    let mut rejected = Box::pin(handle.send_storage_effect(keyed_read("queued")));
    poll_queued(&mut rejected).await;
    serve_next(&mut storage, &receivers.foreground);
    serve_next(&mut storage, &receivers.foreground);
    assert!(matches!(
        committed.await,
        Event::Storage(StorageEvent::ReadResult { value: Some(_), .. })
    ));
    assert!(matches!(
        rejected.await,
        Event::Storage(StorageEvent::ReadResult { value: None, .. })
    ));
}

// The lane hole this fence closes: bulk work queued before the close must not
// commit once the drain gave up on it.
#[tokio::test]
async fn fence_blocks_bulk() {
    let dir = tempdir().unwrap();
    let (handle, receivers) = StorageHandle::new();
    let mut storage = worker(&dir, &handle);
    let bulk = handle.bulk();
    let mut queued = Box::pin(bulk.send_storage_effect(keyed_write("bulk")));
    poll_queued(&mut queued).await;

    handle.close_writes();
    handle.fence_mutations();
    serve_next(&mut storage, &receivers.bulk);

    assert!(matches!(
        queued.await,
        Event::Storage(StorageEvent::Error {
            error: StorageError::Closed
        })
    ));
    assert_eq!(handle.rejected_writes(), 1);
    let mut absent = Box::pin(handle.send_storage_effect(keyed_read("bulk")));
    poll_queued(&mut absent).await;
    serve_next(&mut storage, &receivers.foreground);
    assert!(matches!(
        absent.await,
        Event::Storage(StorageEvent::ReadResult { value: None, .. })
    ));
}

// A fenced transactional write and a fenced queued commit both leave the
// transaction rolled back and its cleanup entry retired.
#[tokio::test]
async fn fence_aborts_txns() {
    let dir = tempdir().unwrap();
    let (handle, receivers) = StorageHandle::new();
    let mut storage = worker(&dir, &handle);
    let open_txn = started_txn(&handle, &receivers, &mut storage).await;
    let commit_txn = started_txn(&handle, &receivers, &mut storage).await;
    let mut staged = Box::pin(handle.send_storage_effect(StorageEffect::Write {
        key_space: "fenced".to_string(),
        key: b"staged".to_vec().into(),
        value: b"value".to_vec().into(),
        txn_id: Some(commit_txn),
    }));
    poll_queued(&mut staged).await;
    serve_next(&mut storage, &receivers.foreground);
    assert!(matches!(
        staged.await,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
    assert_eq!(handle.pending_transactions(), 2);

    let mut fenced_write = Box::pin(handle.send_storage_effect(StorageEffect::Write {
        key_space: "fenced".to_string(),
        key: b"late".to_vec().into(),
        value: b"value".to_vec().into(),
        txn_id: Some(open_txn),
    }));
    poll_queued(&mut fenced_write).await;
    let mut fenced_commit = Box::pin(
        handle.send_storage_effect(StorageEffect::CommitTransaction { txn_id: commit_txn }),
    );
    poll_queued(&mut fenced_commit).await;
    handle.fence_mutations();
    serve_next(&mut storage, &receivers.foreground);
    serve_next(&mut storage, &receivers.foreground);

    assert!(matches!(
        fenced_write.await,
        Event::Storage(StorageEvent::Error {
            error: StorageError::Closed
        })
    ));
    assert!(matches!(
        fenced_commit.await,
        Event::Storage(StorageEvent::Error {
            error: StorageError::Closed
        })
    ));
    assert!(storage.txns.is_empty());
    assert_eq!(handle.pending_transactions(), 0);
    assert!(!handle.commit_unknown(commit_txn));
    let mut staged_read = Box::pin(handle.send_storage_effect(keyed_read("staged")));
    poll_queued(&mut staged_read).await;
    serve_next(&mut storage, &receivers.foreground);
    assert!(matches!(
        staged_read.await,
        Event::Storage(StorageEvent::ReadResult { value: None, .. })
    ));
}

async fn started_txn(
    handle: &StorageHandle,
    receivers: &super::StorageReceivers,
    storage: &mut FjallStorage,
) -> Ulid {
    let mut started =
        Box::pin(handle.send_storage_effect(StorageEffect::StartTransaction { read: false }));
    poll_queued(&mut started).await;
    serve_next(storage, &receivers.foreground);
    match started.await {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        other => panic!("unexpected storage event: {other:?}"),
    }
}

// The worker retry may commit a timed-out commit ahead of its queued original, so it
// must never run before effects the owner awaited, and the original must stay inert.
#[tokio::test]
async fn retry_keeps_order() {
    let dir = tempdir().unwrap();
    let (handle, receivers) = StorageHandle::new();
    let mut storage = worker(&dir, &handle);
    let txn_id = started_txn(&handle, &receivers, &mut storage).await;
    let mut staged = Box::pin(handle.send_storage_effect(StorageEffect::Write {
        key_space: "fenced".to_string(),
        key: b"staged".to_vec().into(),
        value: b"value".to_vec().into(),
        txn_id: Some(txn_id),
    }));
    // No reply before the worker applies the write, so the owner cannot commit ahead of it.
    poll_queued(&mut staged).await;
    serve_next(&mut storage, &receivers.foreground);
    assert!(matches!(
        staged.await,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));

    let mut blocker = Box::pin(handle.send_storage_effect(keyed_write("blocker")));
    poll_queued(&mut blocker).await;
    // The real request timeout elapses while this thread serves nothing.
    assert!(matches!(
        handle
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await,
        Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout
        })
    ));
    assert!(handle.commit_unknown(txn_id));
    assert!(matches!(
        handle
            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
            .await,
        Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict
        })
    ));
    assert_eq!(handle.in_flight(), 2, "blocker and commit stay queued");

    serve_next(&mut storage, &receivers.foreground);
    assert!(storage.txns.is_empty(), "retry commits after the blocker");
    assert_eq!(handle.pending_transactions(), 0);
    assert_eq!(handle.in_flight(), 1, "original commit is still queued");
    serve_next(&mut storage, &receivers.foreground);
    assert_eq!(handle.in_flight(), 0);
    assert_eq!(handle.pending_transactions(), 0);
    assert!(matches!(
        blocker.await,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
    let mut read = Box::pin(handle.send_storage_effect(keyed_read("staged")));
    poll_queued(&mut read).await;
    serve_next(&mut storage, &receivers.foreground);
    assert!(matches!(
        read.await,
        Event::Storage(StorageEvent::ReadResult { value: Some(_), .. })
    ));
}

// A cleanup write that wins channel capacity after the fence is rejected by
// the worker gate rather than committing behind the final fsync.
#[tokio::test]
async fn deferred_respects_fence() {
    let dir = tempdir().unwrap();
    let (handle, receivers) = small_handle(1);
    let mut storage = worker(&dir, &handle);
    let filler = keyed_read("filler");
    let (filler_tx, _filler_rx) = super::response_channel(super::ResponseToken::empty());
    let filler_span = super::storage_effect_span(&filler);
    let filler_guard = super::InFlightGuard::acquire(&handle.metrics);
    handle
        .write_channel
        .try_send((filler, filler_tx, filler_span, Instant::now(), filler_guard))
        .expect("fill queue");
    let waiter = tokio::spawn({
        let handle = handle.clone();
        async move { handle.send_storage_effect(cleanup_write()).await }
    });
    // Two accepted effects means the cleanup write is parked on capacity.
    while handle.in_flight() < 2 {
        tokio::task::yield_now().await;
    }

    let receiver = receivers.foreground.into_async();
    drop(receiver.recv().await.expect("filler effect"));
    let item = receiver.recv().await.expect("cleanup effect");
    assert!(super::is_cleanup_write(&item.0));
    handle.fence_mutations();
    storage.process_single(item, &mut super::SlowQueueAggregator::default());

    assert!(matches!(
        waiter.await.expect("cleanup sender"),
        Event::Storage(StorageEvent::Error {
            error: StorageError::Closed
        })
    ));
}

#[tokio::test]
async fn sync_surfaces_errors() {
    let (handle, receivers) = StorageHandle::new();
    let receiver = receivers.foreground;
    thread::spawn(move || {
        let (effect, response_tx, _span, _enqueued_at, _in_flight) =
            receiver.recv().expect("sync_all effect should arrive");
        assert!(matches!(effect, StorageEffect::SyncAll));
        let _ = response_tx.send(StorageEvent::Error {
            error: StorageError::PersistError("boom".to_string()),
        });
    });

    let error = handle.sync_all().await.expect_err("sync_all should fail");

    assert_eq!(error, StorageError::PersistError("boom".to_string()));
    assert_eq!(handle.get_errors(), 1);
}

pub(super) async fn start_write_transaction(handle: &StorageHandle) -> Ulid {
    match handle
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        other => panic!("unexpected storage event: {other:?}"),
    }
}

#[tokio::test]
async fn owner_aborts_drop() {
    let dir = tempdir().unwrap();
    let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let owner = handle.start_transaction(true).await.unwrap();
    let txn_id = owner.id().unwrap();
    drop(owner);

    assert!(matches!(
        handle
            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
            .await,
        Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionNotFound
        })
    ));
    assert_eq!(handle.pending_transactions(), 0);
}

#[tokio::test]
async fn abort_fence() {
    let (handle, _receivers) = StorageHandle::new();
    let txn_id = Ulid::from_parts(1, 1);
    handle.transaction_cleanup.lock().unwrap().insert(
        txn_id,
        super::CleanupEntry {
            kind: super::CleanupKind::CommitUnknown,
            attempts: 0,
            queued: false,
        },
    );
    assert!(handle.retain_transaction(txn_id, true));

    assert!(matches!(
        handle
            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
            .await,
        Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict
        })
    ));
    assert!(handle.commit_unknown(txn_id));
}

#[test]
fn unknown_open_keeps() {
    let (handle, receivers) = StorageHandle::new();
    let txn_id = Ulid::from_parts(1, 5);
    handle.transaction_cleanup.lock().unwrap().insert(
        txn_id,
        super::CleanupEntry {
            kind: super::CleanupKind::Open,
            attempts: 0,
            queued: false,
        },
    );
    let mut owner = super::TransactionOwner::new(handle, txn_id);

    owner.unknown();
    assert_eq!(owner.id(), Some(txn_id));
    drop(owner);

    let (effect, ..) = receivers
        .foreground
        .try_recv()
        .expect("owner drop should enqueue abort");
    assert!(matches!(
        effect,
        StorageEffect::AbortTransaction { txn_id: got } if got == txn_id
    ));
}

#[tokio::test]
async fn disconnected_clears_open() {
    let (handle, receivers) = StorageHandle::new();
    drop(receivers);
    let txn_id = Ulid::from_parts(1, 2);
    handle.transaction_cleanup.lock().unwrap().insert(
        txn_id,
        super::CleanupEntry {
            kind: super::CleanupKind::Open,
            attempts: 0,
            queued: false,
        },
    );

    assert!(matches!(
        handle
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await,
        Event::Storage(StorageEvent::Error {
            error: StorageError::ChannelClosed
        })
    ));
    assert_eq!(handle.pending_transactions(), 0);
}

#[test]
fn delivery_aborts_start() {
    // Delivery can fail after the actor's liveness precheck.
    let (handle, receivers) = StorageHandle::new();
    let txn_id = Ulid::from_parts(1, 3);
    handle.transaction_cleanup.lock().unwrap().insert(
        txn_id,
        super::CleanupEntry {
            kind: super::CleanupKind::Open,
            attempts: 0,
            queued: false,
        },
    );
    let effect = StorageEffect::StartTransaction { read: true };
    let token = super::ResponseToken::new(&handle, &effect);
    let (response_tx, response_rx) = super::response_channel(token);
    assert!(!response_tx.is_closed());
    drop(response_rx);

    super::FjallStorage::deliver_response(
        response_tx,
        StorageEvent::TransactionStarted { txn_id },
        "start_transaction",
        "transaction_started",
    );

    let (effect, ..) = receivers
        .foreground
        .try_recv()
        .expect("delivery failure should enqueue abort");
    assert!(matches!(
        effect,
        StorageEffect::AbortTransaction { txn_id: got } if got == txn_id
    ));
    assert!(matches!(
        handle.transaction_cleanup.lock().unwrap().get(&txn_id),
        Some(super::CleanupEntry {
            kind: super::CleanupKind::Abort,
            ..
        })
    ));
}

#[test]
fn delivery_finishes_commit() {
    let (handle, _receivers) = StorageHandle::new();
    let txn_id = Ulid::from_parts(1, 4);
    handle.transaction_cleanup.lock().unwrap().insert(
        txn_id,
        super::CleanupEntry {
            kind: super::CleanupKind::CommitQueued,
            attempts: 0,
            queued: false,
        },
    );
    let effect = StorageEffect::CommitTransaction { txn_id };
    let token = super::ResponseToken::new(&handle, &effect);
    let (response_tx, response_rx) = super::response_channel(token);
    drop(response_rx);

    super::FjallStorage::deliver_response(
        response_tx,
        StorageEvent::TransactionCommitted { txn_id },
        "commit_transaction",
        "transaction_committed",
    );

    assert!(handle.transaction_cleanup.lock().unwrap().is_empty());
}

#[test]
fn token_aborts_write() {
    let (handle, receivers) = StorageHandle::new();
    let txn_id = Ulid::from_parts(1, 6);
    handle.transaction_cleanup.lock().unwrap().insert(
        txn_id,
        super::CleanupEntry {
            kind: super::CleanupKind::Open,
            attempts: 0,
            queued: false,
        },
    );
    let effect = StorageEffect::Write {
        key_space: "token".to_string(),
        key: b"key".to_vec().into(),
        value: b"value".to_vec().into(),
        txn_id: Some(txn_id),
    };

    drop(super::ResponseToken::new(&handle, &effect));

    let (effect, ..) = receivers
        .foreground
        .try_recv()
        .expect("dropped write should enqueue abort");
    assert!(matches!(
        effect,
        StorageEffect::AbortTransaction { txn_id: got } if got == txn_id
    ));
}

#[test]
fn commit_aborts_open() {
    let (handle, receivers) = StorageHandle::new();
    let txn_id = Ulid::from_parts(1, 7);
    handle.transaction_cleanup.lock().unwrap().insert(
        txn_id,
        super::CleanupEntry {
            kind: super::CleanupKind::Open,
            attempts: 0,
            queued: false,
        },
    );
    let effect = StorageEffect::CommitTransaction { txn_id };

    drop(super::ResponseToken::new(&handle, &effect));

    let (effect, ..) = receivers
        .foreground
        .try_recv()
        .expect("unaccepted commit should enqueue abort");
    assert!(matches!(
        effect,
        StorageEffect::AbortTransaction { txn_id: got } if got == txn_id
    ));
}

#[test]
fn duplicate_commit_safe() {
    let txn_id = Ulid::from_parts(2, 2);
    let mut pending = std::collections::BTreeMap::new();
    pending.insert(
        txn_id,
        super::CleanupEntry {
            kind: super::CleanupKind::CommitUnknown,
            attempts: 0,
            queued: false,
        },
    );

    assert!(
        super::reserve_cleanup(&mut pending, txn_id, super::CleanupKind::CommitQueued).is_err()
    );
    assert!(matches!(
        pending.get(&txn_id).map(|entry| entry.kind),
        Some(super::CleanupKind::CommitUnknown)
    ));
}

#[test]
fn cleanup_cap_distinct() {
    // Capacity exhaustion is not a conflict, so retry loops must not spin on it.
    let mut pending = std::collections::BTreeMap::new();
    for index in 0..super::MAX_TRANSACTION_CLEANUP {
        pending.insert(
            Ulid::from_parts(index as u64, 0),
            super::CleanupEntry {
                kind: super::CleanupKind::Open,
                attempts: 0,
                queued: false,
            },
        );
    }

    assert!(matches!(
        super::reserve_cleanup(
            &mut pending,
            Ulid::from_parts(u64::MAX, 0),
            super::CleanupKind::Abort,
        ),
        Err(StorageError::CleanupCapacity)
    ));
}

#[test]
fn unknown_commit_runs() {
    let dir = tempdir().unwrap();
    let db = fjall::OptimisticTxDatabase::builder(dir.path())
        .manual_journal_persist(true)
        .open()
        .unwrap();
    let txn_id = Ulid::from_parts(3, 3);
    let txn = db.write_tx().unwrap();
    let transaction_cleanup =
        std::sync::Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new()));
    transaction_cleanup.lock().unwrap().insert(
        txn_id,
        super::CleanupEntry {
            kind: super::CleanupKind::CommitUnknown,
            attempts: 0,
            queued: false,
        },
    );
    let mut storage = super::FjallStorage {
        store: super::Store::new(db),
        persist_policy: FjallPersistPolicy::default(),
        txns: std::collections::HashMap::from([(txn_id, super::Txn::Write(Box::new(txn)))]),
        transaction_cleanup,
        metrics: std::sync::Arc::new(super::StorageMetrics::default()),
        read_pool: Vec::new(),
        next_reader: 0,
        bulk_read_pool: Vec::new(),
        next_bulk_reader: 0,
        pool_threads: Vec::new(),
        compactor: crate::compaction::Compactor::idle(),
        deletes: std::collections::HashMap::new(),
        txn_deletes: std::collections::HashMap::new(),
    };

    let event = storage.commit_transaction(txn_id);
    assert!(matches!(
        &event,
        StorageEvent::TransactionCommitted { txn_id: committed } if *committed == txn_id
    ));
    assert!(storage.txns.is_empty());
    assert!(storage.observe_cleanup(Some((txn_id, super::CleanupKind::CommitQueued)), &event));
    super::finish_cleanup(&storage.transaction_cleanup, txn_id);
    assert!(storage.transaction_cleanup.lock().unwrap().is_empty());
}

#[test]
fn unknown_commit_retires() {
    // A re-commit whose transaction is gone can never progress, so it must
    // count toward the bound instead of holding a cleanup slot forever.
    let dir = tempdir().unwrap();
    let db = fjall::OptimisticTxDatabase::builder(dir.path())
        .manual_journal_persist(true)
        .open()
        .unwrap();
    let txn_id = Ulid::from_parts(3, 5);
    let transaction_cleanup =
        std::sync::Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new()));
    transaction_cleanup.lock().unwrap().insert(
        txn_id,
        super::CleanupEntry {
            kind: super::CleanupKind::CommitUnknown,
            attempts: 0,
            queued: false,
        },
    );
    let mut storage = super::FjallStorage {
        store: super::Store::new(db),
        persist_policy: FjallPersistPolicy::default(),
        txns: std::collections::HashMap::new(),
        transaction_cleanup,
        metrics: std::sync::Arc::new(super::StorageMetrics::default()),
        read_pool: Vec::new(),
        next_reader: 0,
        bulk_read_pool: Vec::new(),
        next_bulk_reader: 0,
        pool_threads: Vec::new(),
        compactor: crate::compaction::Compactor::idle(),
        deletes: std::collections::HashMap::new(),
        txn_deletes: std::collections::HashMap::new(),
    };

    for _ in 0..=super::MAX_CLEANUP_ATTEMPTS {
        storage.retry_cleanup();
    }

    assert!(storage.transaction_cleanup.lock().unwrap().is_empty());
}

#[test]
fn retry_skips_queued() {
    // A queued abort is owned by the in-flight effect; a retry racing it
    // would surface a spurious TransactionNotFound to the caller.
    let dir = tempdir().unwrap();
    let db = fjall::OptimisticTxDatabase::builder(dir.path())
        .manual_journal_persist(true)
        .open()
        .unwrap();
    let txn_id = Ulid::from_parts(3, 4);
    let txn = db.write_tx().unwrap();
    let transaction_cleanup =
        std::sync::Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new()));
    transaction_cleanup.lock().unwrap().insert(
        txn_id,
        super::CleanupEntry {
            kind: super::CleanupKind::Abort,
            attempts: 0,
            queued: true,
        },
    );
    let mut storage = super::FjallStorage {
        store: super::Store::new(db),
        persist_policy: FjallPersistPolicy::default(),
        txns: std::collections::HashMap::from([(txn_id, super::Txn::Write(Box::new(txn)))]),
        transaction_cleanup,
        metrics: std::sync::Arc::new(super::StorageMetrics::default()),
        read_pool: Vec::new(),
        next_reader: 0,
        bulk_read_pool: Vec::new(),
        next_bulk_reader: 0,
        pool_threads: Vec::new(),
        compactor: crate::compaction::Compactor::idle(),
        deletes: std::collections::HashMap::new(),
        txn_deletes: std::collections::HashMap::new(),
    };

    storage.retry_cleanup();
    assert!(storage.txns.contains_key(&txn_id));

    storage
        .transaction_cleanup
        .lock()
        .unwrap()
        .get_mut(&txn_id)
        .unwrap()
        .queued = false;
    storage.retry_cleanup();
    assert!(storage.txns.is_empty());
}

#[test]
fn handoff_marks_queued() {
    let (handle, receivers) = StorageHandle::new();
    let txn_id = Ulid::from_parts(1, 8);
    handle.transaction_cleanup.lock().unwrap().insert(
        txn_id,
        super::CleanupEntry {
            kind: super::CleanupKind::Open,
            attempts: 0,
            queued: false,
        },
    );

    assert!(handle.retain_transaction(txn_id, false));
    receivers
        .foreground
        .try_recv()
        .expect("handoff should enqueue abort");
    assert!(matches!(
        handle.transaction_cleanup.lock().unwrap().get(&txn_id),
        Some(entry) if entry.queued
    ));
}

#[test]
fn queued_commit_fences() {
    let txn_id = Ulid::from_parts(2, 2);
    let mut pending = std::collections::BTreeMap::new();
    pending.insert(
        txn_id,
        super::CleanupEntry {
            kind: super::CleanupKind::Open,
            attempts: 0,
            queued: false,
        },
    );
    assert!(super::reserve_cleanup(&mut pending, txn_id, super::CleanupKind::CommitQueued).is_ok());
    assert!(super::reserve_cleanup(&mut pending, txn_id, super::CleanupKind::Abort).is_err());
}

#[tokio::test]
async fn commit_full_aborts() {
    // A commit the queue rejected never reached storage, so it must stay
    // abortable instead of being fenced as possibly committed.
    let (handle, receivers) = small_handle(1);
    let txn_id = Ulid::from_parts(1, 9);
    handle.transaction_cleanup.lock().unwrap().insert(
        txn_id,
        super::CleanupEntry {
            kind: super::CleanupKind::Open,
            attempts: 0,
            queued: false,
        },
    );
    let filler = StorageEffect::Read {
        key_space: "commit_full".to_string(),
        key: b"filler".to_vec().into(),
        txn_id: None,
    };
    let (filler_tx, _filler_rx) = super::response_channel(super::ResponseToken::empty());
    let filler_span = super::storage_effect_span(&filler);
    let filler_guard = super::InFlightGuard::acquire(&handle.metrics);
    handle
        .write_channel
        .try_send((filler, filler_tx, filler_span, Instant::now(), filler_guard))
        .expect("fill queue");

    let event = handle
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await;

    assert!(matches!(
        event,
        Event::Storage(StorageEvent::Error {
            error: StorageError::QueueFull
        })
    ));
    assert!(!handle.commit_unknown(txn_id));
    assert_eq!(handle.pending_transactions(), 1);
    drop(receivers);
}

#[test]
fn dropped_commit_unknown() {
    // A commit already handed to the worker must be retained for
    // reconciliation, never converted into an abort.
    let (handle, receivers) = StorageHandle::new();
    let txn_id = Ulid::from_parts(1, 10);
    handle.transaction_cleanup.lock().unwrap().insert(
        txn_id,
        super::CleanupEntry {
            kind: super::CleanupKind::CommitQueued,
            attempts: 0,
            queued: true,
        },
    );

    drop(super::ResponseToken::new(
        &handle,
        &StorageEffect::CommitTransaction { txn_id },
    ));

    assert!(handle.commit_unknown(txn_id));
    assert!(receivers.foreground.try_recv().is_err());
}

#[test]
fn terminal_entry_frees() {
    // An owner dropped after storage committed must release its cleanup slot
    // instead of aborting a transaction that already landed.
    let (handle, receivers) = StorageHandle::new();
    let txn_id = Ulid::from_parts(1, 11);
    handle.transaction_cleanup.lock().unwrap().insert(
        txn_id,
        super::CleanupEntry {
            kind: super::CleanupKind::Committed,
            attempts: 0,
            queued: false,
        },
    );

    assert!(handle.retain_transaction(txn_id, false));

    assert_eq!(handle.pending_transactions(), 0);
    assert!(receivers.foreground.try_recv().is_err());
}

#[tokio::test]
async fn transaction_cap() {
    let dir = tempdir().unwrap();
    let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let mut owners = Vec::with_capacity(super::MAX_TRANSACTION_CLEANUP);
    for _ in 0..super::MAX_TRANSACTION_CLEANUP {
        let owner = handle.start_transaction(true).await.unwrap();
        owners.push(owner);
    }

    assert!(matches!(
        handle
            .send_storage_effect(StorageEffect::StartTransaction { read: true })
            .await,
        Event::Storage(StorageEvent::Error {
            error: StorageError::CleanupCapacity
        })
    ));

    for mut owner in owners {
        let txn_id = owner.id().unwrap();
        match handle
            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
            .await
        {
            Event::Storage(StorageEvent::TransactionAborted { .. }) => owner.finish(),
            other => panic!("unexpected storage event: {other:?}"),
        }
    }
    assert_eq!(handle.pending_transactions(), 0);
    assert!(handle.start_transaction(true).await.is_ok());
}

pub(super) async fn commit_transaction(handle: &StorageHandle, txn_id: Ulid) {
    match handle
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { txn_id: committed }) => {
            assert_eq!(committed, txn_id);
        }
        other => panic!("unexpected storage event: {other:?}"),
    }
}

async fn abort_transaction(handle: &StorageHandle, txn_id: Ulid) {
    match handle
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionAborted { txn_id: aborted }) => {
            assert_eq!(aborted, txn_id);
        }
        other => panic!("unexpected storage event: {other:?}"),
    }
}

#[tokio::test]
async fn raw_write_roundtrip() {
    let dir = tempdir().unwrap();
    let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();

    assert_write_result(
        handle
            .send_storage_effect(StorageEffect::Write {
                key_space: "raw_write".to_string(),
                key: b"key".to_vec().into(),
                value: b"value".to_vec().into(),
                txn_id: None,
            })
            .await,
        b"key",
    );

    assert_read_result(
        handle
            .send_storage_effect(StorageEffect::Read {
                key_space: "raw_write".to_string(),
                key: b"key".to_vec().into(),
                txn_id: None,
            })
            .await,
        b"key",
        b"value",
    );
}

#[tokio::test]
async fn raw_batch_ordered() {
    let dir = tempdir().unwrap();
    let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();

    assert_batch_write(
        handle
            .send_storage_effect(StorageEffect::BatchWrite {
                writes: vec![
                    (
                        "raw_batch".to_string(),
                        b"a".to_vec().into(),
                        b"1".to_vec().into(),
                    ),
                    (
                        "raw_batch".to_string(),
                        b"b".to_vec().into(),
                        b"2".to_vec().into(),
                    ),
                ],
                txn_id: None,
            })
            .await,
        &[("raw_batch", b"a"), ("raw_batch", b"b")],
    );

    assert_read_result(
        handle
            .send_storage_effect(StorageEffect::Read {
                key_space: "raw_batch".to_string(),
                key: b"a".to_vec().into(),
                txn_id: None,
            })
            .await,
        b"a",
        b"1",
    );
    assert_read_result(
        handle
            .send_storage_effect(StorageEffect::Read {
                key_space: "raw_batch".to_string(),
                key: b"b".to_vec().into(),
                txn_id: None,
            })
            .await,
        b"b",
        b"2",
    );
}

fn assert_batch_read(event: Event, expected: &[(&[u8], Option<&[u8]>)]) {
    match event {
        Event::Storage(StorageEvent::BatchReadResult { values }) => {
            let actual = values
                .iter()
                .map(|(key, value)| (key.as_ref(), value.as_ref().map(|v| v.as_ref())))
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
        }
        other => panic!("unexpected storage event: {other:?}"),
    }
}

#[tokio::test]
async fn batch_read_ordered() {
    let dir = tempdir().unwrap();
    let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();

    for (key, value) in [(b"a", b"1"), (b"b", b"2")] {
        assert_write_result(
            handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: "batch_read".to_string(),
                    key: key.to_vec().into(),
                    value: value.to_vec().into(),
                    txn_id: None,
                })
                .await,
            key,
        );
    }

    assert_batch_read(
        handle
            .send_storage_effect(StorageEffect::BatchRead {
                reads: vec![
                    ("batch_read".to_string(), b"b".to_vec().into()),
                    ("batch_read".to_string(), b"missing".to_vec().into()),
                    ("batch_read".to_string(), b"a".to_vec().into()),
                ],
                txn_id: None,
            })
            .await,
        &[(b"b", Some(b"2")), (b"missing", None), (b"a", Some(b"1"))],
    );
}

#[tokio::test]
async fn transaction_reads_pending() {
    let dir = tempdir().unwrap();
    let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();

    let txn_id = start_write_transaction(&handle).await;
    assert_write_result(
        handle
            .send_storage_effect(StorageEffect::Write {
                key_space: "batch_read_txn".to_string(),
                key: b"key".to_vec().into(),
                value: b"txn".to_vec().into(),
                txn_id: Some(txn_id),
            })
            .await,
        b"key",
    );

    assert_batch_read(
        handle
            .send_storage_effect(StorageEffect::BatchRead {
                reads: vec![("batch_read_txn".to_string(), b"key".to_vec().into())],
                txn_id: Some(txn_id),
            })
            .await,
        &[(b"key", Some(b"txn"))],
    );

    assert_batch_read(
        handle
            .send_storage_effect(StorageEffect::BatchRead {
                reads: vec![("batch_read_txn".to_string(), b"key".to_vec().into())],
                txn_id: None,
            })
            .await,
        &[(b"key", None)],
    );
}

#[tokio::test]
async fn batch_commit_atomic() {
    let dir = tempdir().unwrap();
    let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();

    assert_write_result(
        handle
            .send_storage_effect(StorageEffect::Write {
                key_space: "batch_write_delete_commit".to_string(),
                key: b"delete".to_vec().into(),
                value: b"old".to_vec().into(),
                txn_id: None,
            })
            .await,
        b"delete",
    );

    let txn_id = start_write_transaction(&handle).await;
    assert_batch_write(
        handle
            .send_storage_effect(StorageEffect::BatchWrite {
                writes: vec![(
                    "batch_write_delete_commit".to_string(),
                    b"write".to_vec().into(),
                    b"new".to_vec().into(),
                )],
                txn_id: Some(txn_id),
            })
            .await,
        &[("batch_write_delete_commit", b"write")],
    );
    assert_batch_delete(
        handle
            .send_storage_effect(StorageEffect::BatchDelete {
                deletes: vec![(
                    "batch_write_delete_commit".to_string(),
                    b"delete".to_vec().into(),
                )],
                txn_id: Some(txn_id),
            })
            .await,
        &[("batch_write_delete_commit", b"delete")],
    );

    assert_batch_read(
        handle
            .send_storage_effect(StorageEffect::BatchRead {
                reads: vec![
                    (
                        "batch_write_delete_commit".to_string(),
                        b"write".to_vec().into(),
                    ),
                    (
                        "batch_write_delete_commit".to_string(),
                        b"delete".to_vec().into(),
                    ),
                ],
                txn_id: None,
            })
            .await,
        &[(b"write", None), (b"delete", Some(b"old"))],
    );

    commit_transaction(&handle, txn_id).await;

    assert_batch_read(
        handle
            .send_storage_effect(StorageEffect::BatchRead {
                reads: vec![
                    (
                        "batch_write_delete_commit".to_string(),
                        b"write".to_vec().into(),
                    ),
                    (
                        "batch_write_delete_commit".to_string(),
                        b"delete".to_vec().into(),
                    ),
                ],
                txn_id: None,
            })
            .await,
        &[(b"write", Some(b"new")), (b"delete", None)],
    );
}

#[tokio::test]
async fn batch_abort_discards() {
    let dir = tempdir().unwrap();
    let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();

    assert_write_result(
        handle
            .send_storage_effect(StorageEffect::Write {
                key_space: "batch_write_delete_abort".to_string(),
                key: b"delete".to_vec().into(),
                value: b"old".to_vec().into(),
                txn_id: None,
            })
            .await,
        b"delete",
    );

    let txn_id = start_write_transaction(&handle).await;
    assert_batch_write(
        handle
            .send_storage_effect(StorageEffect::BatchWrite {
                writes: vec![(
                    "batch_write_delete_abort".to_string(),
                    b"write".to_vec().into(),
                    b"new".to_vec().into(),
                )],
                txn_id: Some(txn_id),
            })
            .await,
        &[("batch_write_delete_abort", b"write")],
    );
    assert_batch_delete(
        handle
            .send_storage_effect(StorageEffect::BatchDelete {
                deletes: vec![(
                    "batch_write_delete_abort".to_string(),
                    b"delete".to_vec().into(),
                )],
                txn_id: Some(txn_id),
            })
            .await,
        &[("batch_write_delete_abort", b"delete")],
    );

    abort_transaction(&handle, txn_id).await;

    assert_batch_read(
        handle
            .send_storage_effect(StorageEffect::BatchRead {
                reads: vec![
                    (
                        "batch_write_delete_abort".to_string(),
                        b"write".to_vec().into(),
                    ),
                    (
                        "batch_write_delete_abort".to_string(),
                        b"delete".to_vec().into(),
                    ),
                ],
                txn_id: None,
            })
            .await,
        &[(b"write", None), (b"delete", Some(b"old"))],
    );
}

async fn iter_keys(
    handle: &StorageHandle,
    key_space: &str,
    prefix: Option<&[u8]>,
    start: Option<IterStart>,
) -> Vec<Vec<u8>> {
    match handle
        .send_storage_effect(StorageEffect::Iter {
            key_space: key_space.to_string(),
            prefix: prefix.map(|p| p.to_vec().into()),
            start,
            limit: 100,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => {
            values.into_iter().map(|(k, _)| k.to_vec()).collect()
        }
        other => panic!("unexpected storage event: {other:?}"),
    }
}

#[tokio::test]
async fn iter_bound_inclusive() {
    let dir = tempdir().unwrap();
    let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();

    for key in [b"p/a", b"p/b", b"p/c"] {
        assert_write_result(
            handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: "iter_start".to_string(),
                    key: key.to_vec().into(),
                    value: b"v".to_vec().into(),
                    txn_id: None,
                })
                .await,
            key,
        );
    }

    let keys = iter_keys(
        &handle,
        "iter_start",
        None,
        Some(IterStart::After(b"p/b".to_vec().into())),
    )
    .await;
    assert_eq!(keys, vec![b"p/c".to_vec()]);

    let keys = iter_keys(
        &handle,
        "iter_start",
        None,
        Some(IterStart::At(b"p/b".to_vec().into())),
    )
    .await;
    assert_eq!(keys, vec![b"p/b".to_vec(), b"p/c".to_vec()]);

    let keys = iter_keys(
        &handle,
        "iter_start",
        Some(b"p/"),
        Some(IterStart::At(b"a".to_vec().into())),
    )
    .await;
    assert_eq!(
        keys,
        vec![b"p/a".to_vec(), b"p/b".to_vec(), b"p/c".to_vec()]
    );
}

#[tokio::test]
async fn last_returns_tail() {
    let dir = tempdir().unwrap();
    let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();

    for key in [b"a".as_slice(), b"c", b"b", b"b/1", b"b/2"] {
        assert_write_result(
            handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: "last_key".to_string(),
                    key: key.to_vec().into(),
                    value: key.to_vec().into(),
                    txn_id: None,
                })
                .await,
            key,
        );
    }

    let Event::Storage(StorageEvent::IterResult { values, .. }) = handle
        .send_storage_effect(StorageEffect::Last {
            key_space: "last_key".to_string(),
            prefix: None,
            txn_id: None,
        })
        .await
    else {
        panic!("expected last result");
    };
    assert_eq!(values, vec![(b"c".to_vec().into(), b"c".to_vec().into())]);

    let Event::Storage(StorageEvent::IterResult { values, .. }) = handle
        .send_storage_effect(StorageEffect::Last {
            key_space: "last_key".to_string(),
            prefix: Some(b"b/".to_vec().into()),
            txn_id: None,
        })
        .await
    else {
        panic!("expected prefixed last result");
    };
    assert_eq!(
        values,
        vec![(b"b/2".to_vec().into(), b"b/2".to_vec().into())]
    );
}

#[tokio::test]
async fn write_during_transaction() {
    let dir = tempdir().unwrap();
    let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();

    assert_write_result(
        handle
            .send_storage_effect(StorageEffect::Write {
                key_space: "raw_conflict".to_string(),
                key: b"key".to_vec().into(),
                value: b"before".to_vec().into(),
                txn_id: None,
            })
            .await,
        b"key",
    );

    let txn_id = start_write_transaction(&handle).await;
    assert_write_result(
        handle
            .send_storage_effect(StorageEffect::Write {
                key_space: "raw_conflict".to_string(),
                key: b"txn-key".to_vec().into(),
                value: b"txn".to_vec().into(),
                txn_id: Some(txn_id),
            })
            .await,
        b"txn-key",
    );

    assert_write_result(
        handle
            .send_storage_effect(StorageEffect::Write {
                key_space: "raw_conflict".to_string(),
                key: b"key".to_vec().into(),
                value: b"after".to_vec().into(),
                txn_id: None,
            })
            .await,
        b"key",
    );

    match handle
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { txn_id: committed }) => {
            assert_eq!(committed, txn_id);
        }
        other => panic!("unexpected storage event: {other:?}"),
    }

    assert_read_result(
        handle
            .send_storage_effect(StorageEffect::Read {
                key_space: "raw_conflict".to_string(),
                key: b"txn-key".to_vec().into(),
                txn_id: None,
            })
            .await,
        b"txn-key",
        b"txn",
    );
}

#[tokio::test]
async fn request_metrics_count() {
    let dir = tempdir().unwrap();
    let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();

    let event = handle
        .send_storage_effect(StorageEffect::Read {
            key_space: "missing".to_string(),
            key: b"key".to_vec().into(),
            txn_id: Some(Ulid::generate()),
        })
        .await;

    assert!(matches!(event, Event::Storage(StorageEvent::Error { .. })));
    assert_eq!(
        handle.snapshot_metrics(),
        super::StorageMetricsSnapshot {
            requests_total: 1,
            errors_total: 1,
            conflicts_total: 0,
            failed_total: 1,
            channel_closed: false,
            closed: false,
            rejected_writes: 0,
            last_error: Some("Transaction not found".to_string()),
        }
    );
}

#[tokio::test]
async fn conflict_metrics_separate() {
    let (handle, receivers) = StorageHandle::new();
    let receiver = receivers.foreground;
    thread::spawn(move || {
        let (effect, response_tx, _span, _enqueued_at, _in_flight) =
            receiver.recv().expect("first effect should arrive");
        assert!(matches!(effect, StorageEffect::CommitTransaction { .. }));
        let _ = response_tx.send(StorageEvent::Error {
            error: StorageError::TransactionNotFound,
        });

        let (effect, response_tx, _span, _enqueued_at, _in_flight) =
            receiver.recv().expect("second effect should arrive");
        assert!(matches!(
            effect,
            StorageEffect::StartTransaction { read: false }
        ));
        let _ = response_tx.send(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        });
    });

    let event = handle
        .send_effect(Effect::Storage(StorageEffect::CommitTransaction {
            txn_id: Ulid::generate(),
        }))
        .await;

    assert!(matches!(event, Event::Storage(StorageEvent::Error { .. })));

    let not_found_metrics = handle.snapshot_metrics();
    assert_eq!(not_found_metrics.requests_total, 1);
    assert_eq!(not_found_metrics.errors_total, 1);
    assert_eq!(not_found_metrics.conflicts_total, 0);
    assert_eq!(not_found_metrics.failed_total, 1);
    assert_eq!(
        not_found_metrics.last_error,
        Some("Transaction not found".to_string())
    );

    let event = handle
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await;

    assert!(matches!(
        event,
        Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        })
    ));

    let metrics_after_conflict = handle.snapshot_metrics();
    assert_eq!(metrics_after_conflict.requests_total, 2);
    assert_eq!(metrics_after_conflict.errors_total, 2);
    assert_eq!(metrics_after_conflict.conflicts_total, 1);
    // A retryable conflict is not a failed request.
    assert_eq!(metrics_after_conflict.failed_total, 1);
    assert_eq!(
        metrics_after_conflict.last_error,
        Some("Transaction conflict".to_string())
    );
}

/// Replaces the worker's compactor with a channel the test owns, so queued
/// compactions are observable without running one.
fn stub_compactor(
    storage: &mut FjallStorage,
) -> std::sync::mpsc::Receiver<crate::compaction::CompactionJob> {
    let (sender, receiver) = std::sync::mpsc::channel();
    storage.compactor = crate::compaction::Compactor::stub(sender);
    receiver
}

fn delete_batch(count: u64) -> StorageEffect {
    StorageEffect::BatchDelete {
        deletes: (0..count)
            .map(|index| {
                (
                    "dht_meta_v2".to_string(),
                    index.to_string().into_bytes().into(),
                )
            })
            .collect(),
        txn_id: None,
    }
}

#[test]
fn deletes_below_threshold() {
    let dir = tempdir().unwrap();
    let (handle, _receivers) = StorageHandle::new();
    let mut storage = worker(&dir, &handle);
    let jobs = stub_compactor(&mut storage);

    let below = crate::compaction::DELETE_THRESHOLD - 1;
    storage.process_effect(delete_batch(below));

    assert!(jobs.try_recv().is_err());
    assert_eq!(storage.deletes.get("dht_meta_v2").copied(), Some(below));
}

#[test]
fn deletes_cross_threshold() {
    let dir = tempdir().unwrap();
    let (handle, _receivers) = StorageHandle::new();
    let mut storage = worker(&dir, &handle);
    let jobs = stub_compactor(&mut storage);

    storage.process_effect(delete_batch(crate::compaction::DELETE_THRESHOLD));

    let job = jobs.try_recv().expect("compaction is queued");
    assert_eq!(job.key_space, "dht_meta_v2");
    assert_eq!(job.deletes, crate::compaction::DELETE_THRESHOLD);
    assert_eq!(storage.deletes.get("dht_meta_v2").copied(), Some(0));
    let active = storage.compactor.active.clone();
    crate::compaction::run_job(
        crate::compaction::CompactionJob {
            key_space: job.key_space,
            deletes: job.deletes,
            run: Box::new(move || {
                (job.run)().expect("keyspace compacts");
                storage.process_effect(delete_batch(crate::compaction::DELETE_THRESHOLD));
                let successor = jobs.try_recv().expect("successor is queued");
                storage.process_effect(delete_batch(crate::compaction::DELETE_THRESHOLD));
                assert!(jobs.try_recv().is_err(), "only one successor is queued");
                crate::compaction::run_job(successor, &storage.compactor.active);
                assert!(
                    storage
                        .compactor
                        .active
                        .lock()
                        .expect("compaction mutex")
                        .is_empty()
                );
                Ok(())
            }),
        },
        &active,
    );
}

#[test]
fn commit_counts_deletes() {
    let dir = tempdir().unwrap();
    let (handle, _receivers) = StorageHandle::new();
    let mut storage = worker(&dir, &handle);
    let _jobs = stub_compactor(&mut storage);
    let StorageEvent::TransactionStarted { txn_id } =
        storage.process_effect(StorageEffect::StartTransaction { read: false })
    else {
        panic!("transaction starts");
    };

    storage.process_effect(StorageEffect::Delete {
        key_space: "dht_meta_v2".to_string(),
        key: b"deadline".to_vec().into(),
        txn_id: Some(txn_id),
    });
    assert!(!storage.deletes.contains_key("dht_meta_v2"));

    storage.process_effect(StorageEffect::CommitTransaction { txn_id });
    assert_eq!(storage.deletes.get("dht_meta_v2").copied(), Some(1));
}

#[test]
fn abort_drops_deletes() {
    let dir = tempdir().unwrap();
    let (handle, _receivers) = StorageHandle::new();
    let mut storage = worker(&dir, &handle);
    let StorageEvent::TransactionStarted { txn_id } =
        storage.process_effect(StorageEffect::StartTransaction { read: false })
    else {
        panic!("transaction starts");
    };
    storage.process_effect(StorageEffect::Delete {
        key_space: "dht_meta_v2".to_string(),
        key: b"deadline".to_vec().into(),
        txn_id: Some(txn_id),
    });

    storage.process_effect(StorageEffect::AbortTransaction { txn_id });

    assert!(!storage.deletes.contains_key("dht_meta_v2"));
    assert!(!storage.txn_deletes.contains_key(&txn_id));
}
