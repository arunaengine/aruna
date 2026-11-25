//! Tests the DHT driver loop with its RPC and storage work.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::test_support::make_node;
use aruna_core::handle::Handle;
use aruna_core::structs::identity::realm::RealmId;
use aruna_storage::storage::handle::StorageHandle;
use aruna_storage::storage::memory::MemoryStorage;
use tempfile::{TempDir, tempdir};

fn make_entry(seed: u8, key: DhtKeyId, expires_at: u64) -> StoredEntry {
    make_entry_rev(seed, key, expires_at, 1)
}

fn make_entry_rev(seed: u8, key: DhtKeyId, expires_at: u64, revision: u64) -> StoredEntry {
    let mut secret = [0u8; 32];
    secret[0] = seed;
    let secret = iroh::SecretKey::from_bytes(&secret);
    let publisher = secret.public();
    let mut realm = [0u8; 32];
    realm[0] = seed;
    let realm_id = RealmId::from_bytes(realm);
    let value = vec![seed];
    let signed = super::super::rpc::signed_record_bytes(
        &key, &publisher, &realm_id, &value, expires_at, revision,
    );
    StoredEntry {
        publisher,
        realm_id,
        value,
        expires_at,
        revision,
        signature: secret.sign(&signed),
        retain_until: expires_at,
    }
}

#[test]
fn response_bounds_reject() {
    let nodes = (1..=K + 1).map(|seed| make_node(seed as u8)).collect();
    assert!(validate_response(&DhtResponse::Nodes { nodes }).is_err());

    let key = DhtKeyId::from_data(b"response-entry-limit");
    let entry = make_entry(1, key, 200);
    let value = super::super::rpc::StoredValue {
        publisher: entry.publisher,
        realm_id: entry.realm_id,
        value: entry.value,
        expires_at: entry.expires_at,
        revision: entry.revision,
        signature: entry.signature,
    };
    assert!(
        validate_response(&DhtResponse::Value {
            entries: vec![value; ENTRIES_PER_KEY + 1],
            closer_nodes: Vec::new(),
        })
        .is_err()
    );
}

#[tokio::test]
async fn replay_skips_write() {
    let (storage, receivers) = StorageHandle::new();
    let receiver = receivers.foreground;
    let key = DhtKeyId::from_data(b"unchanged-replay");
    let entry = make_entry(1, key, unix_timestamp_secs().saturating_add(200));
    let encoded = encode_entries(std::slice::from_ref(&entry)).expect("encode entry");
    let txn_id = TxnId::generate();
    let worker = std::thread::spawn(move || {
        let (effect, response, ..) = receiver.recv().expect("transaction start");
        assert!(matches!(
            effect,
            StorageEffect::StartTransaction { read: false }
        ));
        response.send(StorageEvent::TransactionStarted { txn_id });

        let (effect, response, ..) = receiver.recv().expect("transaction read");
        assert!(matches!(
            effect,
            StorageEffect::Read {
                txn_id: Some(id),
                ..
            } if id == txn_id
        ));
        response.send(StorageEvent::ReadResult {
            key: ByteView::from(key.as_bytes().as_slice()),
            value: Some(ByteView::from(encoded)),
        });

        let (effect, response, ..) = receiver.recv().expect("transaction abort");
        assert!(matches!(
            effect,
            StorageEffect::AbortTransaction { txn_id: id } if id == txn_id
        ));
        response.send(StorageEvent::TransactionAborted { txn_id });
    });

    let mutation = StorageMutation::Merge {
        entries: vec![entry],
    };
    mutate_storage_entries(&storage, 6, StorageStage::InboundPutMerge, key, &mutation)
        .await
        .expect("duplicate succeeds");
    worker.join().expect("storage worker");
}

fn open_storage() -> (TempDir, StorageHandle) {
    let directory = tempdir().expect("create storage directory");
    let storage = MemoryStorage::open();
    (directory, storage)
}

async fn make_driver(seed: u8) -> (DhtDriver, ConnectionPool, TempDir) {
    let endpoint = Endpoint::builder(iroh::endpoint::presets::Minimal)
        .secret_key(iroh::SecretKey::from_bytes(&[seed; 32]))
        .relay_mode(iroh::RelayMode::Disabled)
        .alpns(vec![Alpn::Dht.as_bytes().to_vec()])
        .bind_addr(
            "127.0.0.1:0"
                .parse::<std::net::SocketAddr>()
                .expect("valid bind address"),
        )
        .expect("bind address configures")
        .bind()
        .await
        .expect("endpoint binds");
    let pool = ConnectionPool::new(endpoint.clone(), Default::default());
    let (directory, storage) = open_storage();
    let state = DhtStateMachine::new(
        endpoint.id(),
        endpoint.secret_key().clone(),
        unix_timestamp_secs(),
    );
    let (_cmd_tx, cmd_rx) = mpsc::bounded_blocking_async(CMD_CHANNEL_CAPACITY);
    let (_inbound_tx, inbound_rx) = mpsc::bounded_blocking_async(1);
    let driver = DhtDriver::with_clock(
        state,
        DhtClock::new(),
        endpoint,
        storage,
        pool.clone(),
        cmd_rx,
        inbound_rx,
        CancellationToken::new(),
    );
    (driver, pool, directory)
}

async fn close_driver(driver: DhtDriver, pool: ConnectionPool) {
    driver.shutdown.cancel();
    driver.revision_task.abort();
    let _ = pool.shutdown().await;
}

fn spawn_revision_worker(
    storage: &StorageHandle,
) -> (
    tokio::sync::mpsc::Sender<RevisionRequest>,
    IoReceiver,
    tokio::task::JoinHandle<()>,
) {
    let (io_tx, io_rx) = mpsc::bounded_async(IO_EVENT_CAPACITY);
    let (revision_tx, revision_rx) = tokio::sync::mpsc::channel(CMD_CHANNEL_CAPACITY);
    let worker = tokio::spawn(run_revision_allocator(
        storage.clone(),
        io_tx,
        revision_rx,
        CancellationToken::new(),
    ));
    (revision_tx, io_rx, worker)
}

async fn read_entries(storage: &StorageHandle, key: DhtKeyId) -> Vec<StoredEntry> {
    let event = storage
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: DHT_KEYSPACE.to_string(),
            key: ByteView::from(key.as_bytes().as_slice()),
            txn_id: None,
        }))
        .await;
    let Event::Storage(StorageEvent::ReadResult {
        value: Some(value), ..
    }) = event
    else {
        panic!("expected stored DHT entries: {event:?}");
    };
    decode_entries(&value).expect("decode stored entries")
}

async fn index_entries(storage: &StorageHandle, index: DeadlineIndex) -> Vec<DeadlineEntry> {
    iter_deadlines(storage, 0, StorageStage::CleanupActiveIter, index, 16, None)
        .await
        .unwrap_or_else(|_| panic!("read {index:?} deadline index"))
}

async fn read_count(storage: &StorageHandle) -> u64 {
    let event = storage
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: DHT_META_KEYSPACE.to_string(),
            key: ByteView::from(KEY_COUNT_KEY),
            txn_id: None,
        }))
        .await;
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
        panic!("expected DHT key count: {event:?}");
    };
    match decode_counter(value) {
        Ok(count) => count,
        Err(_) => panic!("decode DHT key count"),
    }
}

async fn write_count(storage: &StorageHandle, count: u64) {
    let event = storage
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: DHT_META_KEYSPACE.to_string(),
            key: ByteView::from(KEY_COUNT_KEY),
            value: ByteView::from(count.to_le_bytes().as_slice()),
            txn_id: None,
        }))
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}

async fn row_exists(storage: &StorageHandle, key: DhtKeyId) -> bool {
    let event = storage
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: DHT_KEYSPACE.to_string(),
            key: ByteView::from(key.as_bytes().as_slice()),
            txn_id: None,
        }))
        .await;
    matches!(
        event,
        Event::Storage(StorageEvent::ReadResult { value: Some(_), .. })
    )
}

async fn write_indexed_row(
    storage: &StorageHandle,
    index: DeadlineIndex,
    entry: DeadlineEntry,
    value: Vec<u8>,
) {
    let event = storage
        .send_effect(Effect::Storage(StorageEffect::BatchWrite {
            writes: vec![
                (
                    DHT_KEYSPACE.to_string(),
                    ByteView::from(entry.key.as_bytes().as_slice()),
                    ByteView::from(value),
                ),
                (
                    DHT_META_KEYSPACE.to_string(),
                    ByteView::from(deadline_key(index, entry)),
                    ByteView::from(Vec::new()),
                ),
            ],
            txn_id: None,
        }))
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));
}

async fn store_due_floor(
    storage: &StorageHandle,
    key: DhtKeyId,
    seed: u8,
    expires_at: u64,
) -> DeadlineEntry {
    let entry = make_entry(seed, key, expires_at);
    let deadline = DeadlineEntry {
        deadline: entry.retain_until,
        key,
    };
    write_indexed_row(
        storage,
        DeadlineIndex::Floor,
        deadline,
        encode_entries(std::slice::from_ref(&entry)).unwrap(),
    )
    .await;
    deadline
}

#[tokio::test]
async fn ready_lanes_rotate() {
    let (cmd_tx, mut cmd_rx) = mpsc::bounded_blocking_async(4);
    let (_inbound_tx, mut inbound_rx) = mpsc::bounded_blocking_async(1);
    let (io_tx, mut io_rx) = mpsc::bounded_async(4);
    cmd_tx
        .try_send(DriverCmd::AddPeer {
            node_id: make_node(1),
        })
        .expect("queue first command");
    cmd_tx
        .try_send(DriverCmd::AddPeer {
            node_id: make_node(2),
        })
        .expect("queue second command");
    io_tx
        .send(DhtIo::PeerSeen { peer: make_node(3) })
        .await
        .expect("queue IO event");

    let shutdown = CancellationToken::new();
    let mut ticker = tokio::time::interval(Duration::from_secs(3_600));
    let _ = ticker.tick().await;
    let (first, next_lane) = next_driver_event(
        DriverLane::Command,
        &shutdown,
        &mut ticker,
        None,
        &mut cmd_rx,
        &mut inbound_rx,
        &mut io_rx,
    )
    .await;
    assert!(matches!(first, DriverEvent::Command(_)));

    let (second, _) = next_driver_event(
        next_lane,
        &shutdown,
        &mut ticker,
        None,
        &mut cmd_rx,
        &mut inbound_rx,
        &mut io_rx,
    )
    .await;
    assert!(matches!(second, DriverEvent::Io(_)));
}

#[tokio::test(start_paused = true)]
async fn deadlines_expire_order() {
    let mut deadlines = OpDeadlines::default();
    let start = TokioInstant::now();
    deadlines.insert(7, start + Duration::from_secs(4));
    deadlines.insert(8, start + Duration::from_secs(1));

    assert_eq!(deadlines.next(), Some(start + Duration::from_secs(1)));
    assert!(deadlines.expired(start).is_empty());
    assert_eq!(deadlines.expired(start + Duration::from_secs(1)), vec![8]);

    // A completed operation stops holding the driver's next wakeup.
    deadlines.remove(7);
    assert_eq!(deadlines.next(), None);
    assert_eq!(deadlines.len(), 0);
}

#[tokio::test(start_paused = true)]
async fn driver_deadline_cleans() {
    let (mut driver, pool, _directory) = make_driver(201).await;
    let peer = make_node(202);
    driver.handle_driver_cmd(DriverCmd::AddPeer { node_id: peer });
    let (reply, result) = oneshot::channel();
    driver.handle_driver_cmd(DriverCmd::Get {
        key: DhtKeyId::from_data(b"driver-deadline"),
        realm_filter: None,
        options: DhtGetOptions::exhaustive(Duration::from_secs(1)),
        trace_context: None,
        reply,
    });
    driver.process_input(DhtInput::Io(DhtIo::StorageReadResult {
        op_id: 1,
        stage: StorageStage::GetLocalRead,
        entries: Vec::new(),
    }));

    assert_eq!(driver.pending_callers.len(), 1);
    assert_eq!(driver.deadlines.len(), 1);
    assert!(driver.state.contains_op(1));

    tokio::time::advance(Duration::from_secs(1)).await;
    driver.release_expired();

    assert_eq!(driver.pending_callers.len(), 0);
    assert_eq!(driver.deadlines.len(), 0);
    assert!(!driver.state.contains_op(1));
    assert!(matches!(result.await, Ok(Err(DhtIoError::Timeout))));

    driver.process_input(DhtInput::Io(DhtIo::RpcResponse {
        op_id: 1,
        phase: RpcPhase::GetLookup,
        peer,
        response: DhtResponse::Nodes { nodes: Vec::new() },
    }));
    assert_eq!(driver.pending_callers.len(), 0);
    assert_eq!(driver.deadlines.len(), 0);
    assert!(!driver.state.contains_op(1));
    close_driver(driver, pool).await;
}

#[tokio::test(start_paused = true)]
async fn driver_drop_cleans() {
    let (mut driver, pool, _directory) = make_driver(203).await;
    let peer = make_node(204);
    driver.handle_driver_cmd(DriverCmd::AddPeer { node_id: peer });
    let (reply, result) = oneshot::channel();
    drop(result);
    driver.handle_driver_cmd(DriverCmd::Get {
        key: DhtKeyId::from_data(b"driver-drop"),
        realm_filter: None,
        options: DhtGetOptions::exhaustive(Duration::from_secs(10)),
        trace_context: None,
        reply,
    });
    driver.process_input(DhtInput::Io(DhtIo::StorageReadResult {
        op_id: 1,
        stage: StorageStage::GetLocalRead,
        entries: Vec::new(),
    }));

    assert_eq!(driver.pending_callers.len(), 1);
    assert_eq!(driver.deadlines.len(), 1);
    assert!(driver.state.contains_op(1));

    driver.release_abandoned();

    assert_eq!(driver.pending_callers.len(), 0);
    assert_eq!(driver.deadlines.len(), 0);
    assert!(!driver.state.contains_op(1));
    driver.process_input(DhtInput::Io(DhtIo::RpcResponse {
        op_id: 1,
        phase: RpcPhase::GetLookup,
        peer,
        response: DhtResponse::Nodes { nodes: Vec::new() },
    }));
    assert_eq!(driver.pending_callers.len(), 0);
    assert_eq!(driver.deadlines.len(), 0);
    assert!(!driver.state.contains_op(1));
    close_driver(driver, pool).await;
}

#[test]
fn request_size_bound() {
    let request = DhtRequest::PutValue {
        key: DhtKeyId::from_data(b"oversized-request"),
        realm_id: RealmId::from_bytes([1u8; 32]),
        value: vec![0; MAX_MESSAGE_SIZE],
        expires_at: 1,
        revision: 1,
        publisher: make_node(1),
        signature: None,
    };

    assert!(matches!(
        encode_request_frame(&request, None),
        Err(DhtIoError::InvalidRequest(message)) if message == "request too large"
    ));
}

#[test]
fn inbound_budget_precedes() {
    assert!(INBOUND_STORAGE_BUDGET < RPC_TIMEOUT);
}

#[tokio::test]
async fn revision_blocks_batch() {
    let (_directory, storage) = open_storage();
    let (revision_tx, io_rx, worker) = spawn_revision_worker(&storage);
    let count = REVISION_BLOCK_SIZE + 1;
    for op_id in 1..=count {
        revision_tx
            .send(RevisionRequest {
                op_id,
                stage: StorageStage::PutRevision,
                span: Span::current(),
            })
            .await
            .expect("queue revision");
    }

    for expected in 1..=count {
        assert!(matches!(
            io_rx.recv().await,
            Ok(DhtIo::StorageRevisionResult {
                op_id,
                stage: StorageStage::PutRevision,
                revision,
            }) if op_id == expected && revision == expected
        ));
    }
    drop(revision_tx);
    worker.await.expect("revision worker");

    assert_eq!(storage.snapshot_metrics().conflicts_total, 0);
    assert_eq!(storage.snapshot_metrics().requests_total, 8);
}

#[tokio::test]
async fn revision_restart_skips() {
    let (_directory, storage) = open_storage();
    let (first_tx, first_rx, first_worker) = spawn_revision_worker(&storage);
    first_tx
        .send(RevisionRequest {
            op_id: 1,
            stage: StorageStage::PutRevision,
            span: Span::current(),
        })
        .await
        .expect("queue first revision");
    assert!(matches!(
        first_rx.recv().await,
        Ok(DhtIo::StorageRevisionResult { revision: 1, .. })
    ));
    drop(first_tx);
    first_worker.await.expect("first revision worker");

    let (second_tx, second_rx, second_worker) = spawn_revision_worker(&storage);
    second_tx
        .send(RevisionRequest {
            op_id: 2,
            stage: StorageStage::PutRevision,
            span: Span::current(),
        })
        .await
        .expect("queue second revision");
    assert!(matches!(
        second_rx.recv().await,
        Ok(DhtIo::StorageRevisionResult { revision: 65, .. })
    ));
    drop(second_tx);
    second_worker.await.expect("second revision worker");

    assert_eq!(storage.snapshot_metrics().requests_total, 8);
}

#[tokio::test]
async fn large_keyspace_admits() {
    let (_directory, storage) = open_storage();
    let count = 65_536;
    write_count(&storage, count).await;
    let key = DhtKeyId::from_data(b"past-former-key-limit");
    let mutation = StorageMutation::Merge {
        entries: vec![make_entry(
            1,
            key,
            unix_timestamp_secs().saturating_add(200),
        )],
    };

    mutate_storage_entries(&storage, 12, StorageStage::InboundPutMerge, key, &mutation)
        .await
        .expect("admit beyond the former key limit");

    assert!(row_exists(&storage, key).await);
    assert_eq!(read_count(&storage).await, count + 1);
    assert_eq!(
        index_entries(&storage, DeadlineIndex::Active)
            .await
            .into_iter()
            .map(|entry| entry.key)
            .collect::<Vec<_>>(),
        vec![key]
    );
}

#[tokio::test]
async fn key_limit_enforced() {
    let (_directory, storage) = open_storage();
    let event = storage
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: DHT_META_KEYSPACE.to_string(),
            key: ByteView::from(KEY_COUNT_KEY),
            value: ByteView::from(MAX_STORED_KEYS.to_le_bytes().as_slice()),
            txn_id: None,
        }))
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));

    let key = DhtKeyId::from_data(b"full-keyspace");
    let mutation = StorageMutation::Merge {
        entries: vec![make_entry(
            1,
            key,
            unix_timestamp_secs().saturating_add(200),
        )],
    };
    assert!(matches!(
        mutate_storage_entries(&storage, 12, StorageStage::InboundPutMerge, key, &mutation,).await,
        Err(DhtIoError::StorageFull)
    ));

    let event = storage
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: DHT_KEYSPACE.to_string(),
            key: ByteView::from(key.as_bytes().as_slice()),
            txn_id: None,
        }))
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::ReadResult { value: None, .. })
    ));
}

#[tokio::test]
async fn capacity_retained_floor() {
    let (_directory, storage) = open_storage();
    let now_secs = unix_timestamp_secs();
    let victim = DhtKeyId::from_data(b"floor-victim");
    let target = DhtKeyId::from_data(b"floor-replacement");
    let retained = StorageMutation::Merge {
        entries: vec![make_entry(1, victim, now_secs.saturating_sub(1))],
    };
    mutate_storage_entries(
        &storage,
        30,
        StorageStage::InboundPutMerge,
        victim,
        &retained,
    )
    .await
    .expect("store retained floor");
    write_count(&storage, MAX_STORED_KEYS).await;
    let mutation = StorageMutation::Merge {
        entries: vec![make_entry(2, target, now_secs.saturating_add(200))],
    };

    assert!(matches!(
        mutate_storage_entries(
            &storage,
            32,
            StorageStage::InboundPutMerge,
            target,
            &mutation,
        )
        .await,
        Err(DhtIoError::StorageFull)
    ));

    assert!(row_exists(&storage, victim).await);
    assert!(!row_exists(&storage, target).await);
    assert_eq!(read_count(&storage).await, MAX_STORED_KEYS);
    assert_eq!(
        index_entries(&storage, DeadlineIndex::Floor)
            .await
            .into_iter()
            .map(|entry| entry.key)
            .collect::<Vec<_>>(),
        vec![victim]
    );
}

#[tokio::test]
async fn capacity_expired_row() {
    let (_directory, storage) = open_storage();
    let now_secs = unix_timestamp_secs();
    let victim = DhtKeyId::from_data(b"expired-active");
    let target = DhtKeyId::from_data(b"expired-replacement");
    let entry = make_entry(1, victim, now_secs);
    let deadline = DeadlineEntry {
        deadline: now_secs,
        key: victim,
    };
    write_indexed_row(
        &storage,
        DeadlineIndex::Active,
        deadline,
        encode_entries(std::slice::from_ref(&entry)).unwrap(),
    )
    .await;
    write_count(&storage, MAX_STORED_KEYS).await;
    let mutation = StorageMutation::Merge {
        entries: vec![make_entry(2, target, now_secs.saturating_add(200))],
    };

    mutate_storage_entries(
        &storage,
        33,
        StorageStage::InboundPutMerge,
        target,
        &mutation,
    )
    .await
    .expect("replace overdue active row");

    assert!(!row_exists(&storage, victim).await);
    assert!(row_exists(&storage, target).await);
    assert_eq!(read_count(&storage).await, MAX_STORED_KEYS);
}

#[tokio::test]
async fn capacity_unsafe_rows() {
    // Capacity reclaim skips corrupt, invalid, and live rows before choosing a safe victim.
    let (_directory, storage) = open_storage();
    let now_secs = unix_timestamp_secs();
    let corrupt = DhtKeyId::from_data(b"corrupt-victim");
    let invalid_key = DhtKeyId::from_data(b"invalid-victim");
    let victim = DhtKeyId::from_data(b"live-victim");
    let floor_victim = DhtKeyId::from_data(b"later-floor-victim");
    let target = DhtKeyId::from_data(b"safe-replacement");
    let _ = store_due_floor(&storage, floor_victim, 2, now_secs.saturating_sub(1)).await;
    let expires_at = now_secs.saturating_add(200);
    let entry = make_entry(1, victim, expires_at);
    let mut invalid = make_entry(4, invalid_key, expires_at);
    invalid.revision = 0;
    write_indexed_row(
        &storage,
        DeadlineIndex::Floor,
        DeadlineEntry {
            deadline: now_secs.saturating_sub(3),
            key: corrupt,
        },
        b"not-postcard".to_vec(),
    )
    .await;
    write_indexed_row(
        &storage,
        DeadlineIndex::Floor,
        DeadlineEntry {
            deadline: now_secs.saturating_sub(3),
            key: invalid_key,
        },
        encode_entries(std::slice::from_ref(&invalid)).unwrap(),
    )
    .await;
    write_indexed_row(
        &storage,
        DeadlineIndex::Floor,
        DeadlineEntry {
            deadline: now_secs.saturating_sub(2),
            key: victim,
        },
        encode_entries(std::slice::from_ref(&entry)).unwrap(),
    )
    .await;
    write_count(&storage, MAX_STORED_KEYS).await;
    let mutation = StorageMutation::Merge {
        entries: vec![make_entry(3, target, now_secs.saturating_add(300))],
    };

    mutate_storage_entries(
        &storage,
        35,
        StorageStage::InboundPutMerge,
        target,
        &mutation,
    )
    .await
    .expect("replace expired floor row");

    assert!(row_exists(&storage, corrupt).await);
    assert!(row_exists(&storage, invalid_key).await);
    assert!(row_exists(&storage, victim).await);
    assert!(!row_exists(&storage, floor_victim).await);
    assert!(row_exists(&storage, target).await);
    assert_eq!(read_count(&storage).await, MAX_STORED_KEYS);
    assert!(
        index_entries(&storage, DeadlineIndex::Floor)
            .await
            .is_empty()
    );
    let active = index_entries(&storage, DeadlineIndex::Active).await;
    assert_eq!(active.len(), 2);
    assert!(active.iter().any(|entry| entry.key == victim));
    assert!(active.iter().any(|entry| entry.key == target));
}

#[tokio::test]
async fn capacity_floor_admission() {
    let (_directory, storage) = open_storage();
    let now_secs = unix_timestamp_secs();
    let victim = DhtKeyId::from_data(b"retained-floor");
    let target = DhtKeyId::from_data(b"new-floor");
    let _ = store_due_floor(&storage, victim, 1, now_secs.saturating_sub(2)).await;
    write_count(&storage, MAX_STORED_KEYS).await;
    let mutation = StorageMutation::Merge {
        entries: vec![make_entry(2, target, now_secs.saturating_sub(1))],
    };

    assert!(matches!(
        mutate_storage_entries(
            &storage,
            38,
            StorageStage::InboundPutMerge,
            target,
            &mutation,
        )
        .await,
        Err(DhtIoError::StorageFull)
    ));
    assert!(row_exists(&storage, victim).await);
    assert!(!row_exists(&storage, target).await);
    assert_eq!(read_count(&storage).await, MAX_STORED_KEYS);
}

#[tokio::test]
async fn capacity_concurrent_reclaims() {
    let (_directory, storage) = open_storage();
    let now_secs = unix_timestamp_secs();
    let first_victim = DhtKeyId::from_data(b"first-floor-victim");
    let second_victim = DhtKeyId::from_data(b"second-floor-victim");
    let first_target = DhtKeyId::from_data(b"first-admission");
    let second_target = DhtKeyId::from_data(b"second-admission");
    let _ = store_due_floor(&storage, first_victim, 1, now_secs.saturating_sub(2)).await;
    let _ = store_due_floor(&storage, second_victim, 2, now_secs.saturating_sub(1)).await;
    write_count(&storage, MAX_STORED_KEYS).await;
    let first = StorageMutation::Merge {
        entries: vec![make_entry(3, first_target, now_secs.saturating_add(200))],
    };
    let second = StorageMutation::Merge {
        entries: vec![make_entry(4, second_target, now_secs.saturating_add(300))],
    };

    let (first_result, second_result) = tokio::join!(
        mutate_storage_entries(
            &storage,
            44,
            StorageStage::InboundPutMerge,
            first_target,
            &first,
        ),
        mutate_storage_entries(
            &storage,
            45,
            StorageStage::InboundPutMerge,
            second_target,
            &second,
        ),
    );
    first_result.expect("first admission succeeds");
    second_result.expect("second admission retries");
    assert!(
        storage.snapshot_metrics().conflicts_total > 0,
        "admissions overlap"
    );

    assert!(!row_exists(&storage, first_victim).await);
    assert!(!row_exists(&storage, second_victim).await);
    assert!(row_exists(&storage, first_target).await);
    assert!(row_exists(&storage, second_target).await);
    assert_eq!(read_count(&storage).await, MAX_STORED_KEYS);
    assert!(
        index_entries(&storage, DeadlineIndex::Floor)
            .await
            .is_empty()
    );
    assert_eq!(
        index_entries(&storage, DeadlineIndex::Active).await.len(),
        2
    );
}

#[tokio::test]
async fn merge_indexes_row() {
    let (_directory, storage) = open_storage();
    let key = DhtKeyId::from_data(b"indexed-row");
    let expires_at = unix_timestamp_secs().saturating_add(200);
    let mutation = StorageMutation::Merge {
        entries: vec![make_entry(1, key, expires_at)],
    };

    mutate_storage_entries(&storage, 14, StorageStage::InboundPutMerge, key, &mutation)
        .await
        .expect("store indexed row");

    assert_eq!(
        index_entries(&storage, DeadlineIndex::Active).await,
        vec![DeadlineEntry {
            deadline: expires_at,
            key
        }]
    );
    assert!(
        index_entries(&storage, DeadlineIndex::Floor)
            .await
            .is_empty()
    );
    assert_eq!(read_count(&storage).await, 1);
}

#[tokio::test]
async fn cleanup_moves_floor() {
    let (_directory, storage) = open_storage();
    let key = DhtKeyId::from_data(b"floor-transition");
    let expires_at = unix_timestamp_secs().saturating_add(20);
    let mutation = StorageMutation::Merge {
        entries: vec![make_entry(1, key, expires_at)],
    };
    mutate_storage_entries(&storage, 15, StorageStage::InboundPutMerge, key, &mutation)
        .await
        .expect("store active row");
    let before = read_entries(&storage, key).await;
    let active = index_entries(&storage, DeadlineIndex::Active).await[0];
    let prune = StorageMutation::Prune {
        index: DeadlineIndex::Active,
        entry: active,
        now_secs: expires_at,
    };

    mutate_storage_entries(&storage, 16, StorageStage::CleanupActivePrune, key, &prune)
        .await
        .expect("move row to floor index");

    assert_eq!(read_entries(&storage, key).await, before);
    assert!(
        index_entries(&storage, DeadlineIndex::Active)
            .await
            .is_empty()
    );
    assert_eq!(index_entries(&storage, DeadlineIndex::Floor).await.len(), 1);
    assert_eq!(read_count(&storage).await, 1);
}

#[tokio::test]
async fn cleanup_deletes_floor() {
    let (_directory, storage) = open_storage();
    let key = DhtKeyId::from_data(b"floor-removal");
    let expires_at = unix_timestamp_secs().saturating_add(20);
    let mutation = StorageMutation::Merge {
        entries: vec![make_entry(1, key, expires_at)],
    };
    mutate_storage_entries(&storage, 17, StorageStage::InboundPutMerge, key, &mutation)
        .await
        .expect("store active row");
    let active = index_entries(&storage, DeadlineIndex::Active).await[0];
    let prune = StorageMutation::Prune {
        index: DeadlineIndex::Active,
        entry: active,
        now_secs: expires_at,
    };
    mutate_storage_entries(&storage, 18, StorageStage::CleanupActivePrune, key, &prune)
        .await
        .expect("move row to floor index");
    let floor = index_entries(&storage, DeadlineIndex::Floor).await[0];
    let prune = StorageMutation::Prune {
        index: DeadlineIndex::Floor,
        entry: floor,
        now_secs: floor.deadline,
    };

    mutate_storage_entries(&storage, 19, StorageStage::CleanupFloorPrune, key, &prune)
        .await
        .expect("delete expired floor");

    assert!(
        index_entries(&storage, DeadlineIndex::Active)
            .await
            .is_empty()
    );
    assert!(
        index_entries(&storage, DeadlineIndex::Floor)
            .await
            .is_empty()
    );
    assert_eq!(read_count(&storage).await, 0);
    let event = storage
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: DHT_KEYSPACE.to_string(),
            key: ByteView::from(key.as_bytes().as_slice()),
            txn_id: None,
        }))
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::ReadResult { value: None, .. })
    ));
}

#[tokio::test]
async fn cleanup_clears_orphan() {
    let (_directory, storage) = open_storage();
    let key = DhtKeyId::from_data(b"orphan-index");
    let entry = DeadlineEntry { deadline: 10, key };
    let raw_key = deadline_key(DeadlineIndex::Floor, entry);
    let event = storage
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: DHT_META_KEYSPACE.to_string(),
            key: ByteView::from(raw_key),
            value: ByteView::from(Vec::new()),
            txn_id: None,
        }))
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
    let prune = StorageMutation::Prune {
        index: DeadlineIndex::Floor,
        entry,
        now_secs: entry.deadline,
    };

    mutate_storage_entries(&storage, 20, StorageStage::CleanupFloorPrune, key, &prune)
        .await
        .expect("remove orphan index");

    assert!(
        index_entries(&storage, DeadlineIndex::Floor)
            .await
            .is_empty()
    );
    assert_eq!(read_count(&storage).await, 0);
}

#[tokio::test]
async fn cleanup_skips_bad() {
    let (_directory, storage) = open_storage();
    let key = DhtKeyId::from_data(b"valid-index");
    let entry = DeadlineEntry { deadline: 10, key };
    let mut invalid = DeadlineIndex::Active.prefix().to_vec();
    invalid.extend_from_slice(b"bad");
    for raw_key in [invalid.clone(), deadline_key(DeadlineIndex::Active, entry)] {
        let event = storage
            .send_effect(Effect::Storage(StorageEffect::Write {
                key_space: DHT_META_KEYSPACE.to_string(),
                key: ByteView::from(raw_key),
                value: ByteView::from(Vec::new()),
                txn_id: None,
            }))
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    assert_eq!(
        index_entries(&storage, DeadlineIndex::Active).await,
        vec![entry]
    );
    let event = storage
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: DHT_META_KEYSPACE.to_string(),
            key: ByteView::from(invalid),
            txn_id: None,
        }))
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::ReadResult { value: None, .. })
    ));
}

#[tokio::test]
async fn cleanup_preserves_corrupt() {
    let decode_key = DhtKeyId::from_data(b"corrupt-cleanup");
    let invalid_key = DhtKeyId::from_data(b"invalid-cleanup");
    let mut invalid = make_entry(1, invalid_key, 10);
    invalid.revision = 0;
    let cases = [
        (decode_key, b"not-postcard".to_vec()),
        (
            invalid_key,
            encode_entries(std::slice::from_ref(&invalid)).expect("encode invalid row"),
        ),
    ];
    for (key, value) in cases {
        let (_directory, storage) = open_storage();
        let entry = DeadlineEntry { deadline: 10, key };
        let writes = vec![
            (
                DHT_KEYSPACE.to_string(),
                ByteView::from(key.as_bytes().as_slice()),
                ByteView::from(value.as_slice()),
            ),
            (
                DHT_META_KEYSPACE.to_string(),
                ByteView::from(deadline_key(DeadlineIndex::Active, entry)),
                ByteView::from(Vec::new()),
            ),
            (
                DHT_META_KEYSPACE.to_string(),
                ByteView::from(KEY_COUNT_KEY),
                ByteView::from(1u64.to_le_bytes().as_slice()),
            ),
        ];
        let event = storage
            .send_effect(Effect::Storage(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            }))
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::BatchWriteResult { .. })
        ));
        let prune = StorageMutation::Prune {
            index: DeadlineIndex::Active,
            entry,
            now_secs: entry.deadline,
        };

        mutate_storage_entries(&storage, 21, StorageStage::CleanupActivePrune, key, &prune)
            .await
            .expect("quarantine corrupt row");

        assert!(
            index_entries(&storage, DeadlineIndex::Active)
                .await
                .is_empty()
        );
        assert_eq!(read_count(&storage).await, 1);
        let event = storage
            .send_effect(Effect::Storage(StorageEffect::Read {
                key_space: DHT_KEYSPACE.to_string(),
                key: ByteView::from(key.as_bytes().as_slice()),
                txn_id: None,
            }))
            .await;
        let Event::Storage(StorageEvent::ReadResult {
            value: Some(stored),
            ..
        }) = event
        else {
            panic!("expected preserved DHT row: {event:?}");
        };
        assert_eq!(stored.as_ref(), value);
    }
}

#[tokio::test]
async fn batch_merge_persists() {
    let (_directory, storage) = open_storage();
    let key = DhtKeyId::from_data(b"batch-remote-merge");
    let expires_at = unix_timestamp_secs().saturating_add(200);
    let mutation = StorageMutation::Merge {
        entries: vec![
            make_entry(1, key, expires_at),
            make_entry(2, key, expires_at),
        ],
    };
    let clock = DhtClock::new();

    mutate_storage_checked(
        &storage,
        13,
        StorageStage::GetRemoteMerge,
        key,
        &mutation,
        &clock,
        None,
    )
    .await
    .expect("batch merge succeeds");

    let entries = read_entries(&storage, key).await;
    assert_eq!(entries.len(), 2);
}

#[tokio::test]
async fn writeback_drops_stale() {
    // A read-repair batch commits the entries that still win and silently drops
    // the one that lost a concurrent revision race, rather than failing wholesale.
    let (_directory, storage) = open_storage();
    let key = DhtKeyId::from_data(b"read-repair-mixed-batch");
    let expires_at = unix_timestamp_secs().saturating_add(200);

    let winner = StorageMutation::Merge {
        entries: vec![make_entry_rev(1, key, expires_at, 2)],
    };
    mutate_storage_entries(&storage, 20, StorageStage::InboundPutMerge, key, &winner)
        .await
        .expect("seed winning revision");

    let clock = DhtClock::new();
    let batch = StorageMutation::Merge {
        entries: vec![
            make_entry_rev(1, key, expires_at, 1),
            make_entry(2, key, expires_at),
        ],
    };
    mutate_storage_checked(
        &storage,
        21,
        StorageStage::GetRemoteMerge,
        key,
        &batch,
        &clock,
        None,
    )
    .await
    .expect("read-repair commits fresh subset");

    let entries = read_entries(&storage, key).await;
    assert_eq!(entries.len(), 2);
    assert!(entries.iter().any(|entry| entry.value == vec![2]));
    assert!(entries.iter().any(|entry| entry.revision == 2));
}

#[tokio::test]
async fn concurrent_merges_preserve() {
    let (_directory, storage) = open_storage();
    let key = DhtKeyId::from_data(b"concurrent-merges");
    let expires_at = unix_timestamp_secs().saturating_add(200);
    let first = StorageMutation::Merge {
        entries: vec![make_entry(1, key, expires_at)],
    };
    let second = StorageMutation::Merge {
        entries: vec![make_entry(2, key, expires_at)],
    };

    let (first_result, second_result) = tokio::join!(
        mutate_storage_entries(&storage, 1, StorageStage::PutLocalMerge, key, &first),
        mutate_storage_entries(&storage, 2, StorageStage::InboundPutMerge, key, &second),
    );
    first_result.expect("first merge succeeds");
    second_result.expect("second merge retries and succeeds");
    assert!(
        storage.snapshot_metrics().conflicts_total > 0,
        "merges overlap"
    );

    let entries = read_entries(&storage, key).await;
    assert_eq!(entries.len(), 2);
}

#[tokio::test]
async fn cleanup_preserves_fresh() {
    let (_directory, storage) = open_storage();
    let key = DhtKeyId::from_data(b"cleanup-race");
    let now_secs = unix_timestamp_secs();
    let old = StorageMutation::Merge {
        entries: vec![make_entry(1, key, now_secs.saturating_add(50))],
    };
    mutate_storage_entries(&storage, 3, StorageStage::InboundPutMerge, key, &old)
        .await
        .expect("store old entry");

    let entry = DeadlineEntry {
        deadline: now_secs.saturating_add(50),
        key,
    };
    let prune = StorageMutation::Prune {
        index: DeadlineIndex::Active,
        entry,
        now_secs: now_secs.saturating_add(400),
    };
    let merge = StorageMutation::Merge {
        entries: vec![make_entry(2, key, now_secs.saturating_add(500))],
    };
    let (prune_result, merge_result) = tokio::join!(
        mutate_storage_entries(&storage, 3, StorageStage::CleanupActivePrune, key, &prune,),
        mutate_storage_entries(&storage, 4, StorageStage::InboundPutMerge, key, &merge),
    );
    prune_result.expect("cleanup retries and succeeds");
    merge_result.expect("fresh merge succeeds");

    let entries = read_entries(&storage, key).await;
    assert_eq!(entries.len(), 2);
    assert!(entries.iter().any(|entry| entry.value == vec![2]));
    assert!(entries.iter().any(|entry| entry.value == vec![1]));
}

#[tokio::test]
async fn corrupt_row_preserved() {
    let (_directory, storage) = open_storage();
    let key = DhtKeyId::from_data(b"corrupt-row");
    let corrupt = ByteView::from(b"not-postcard".as_slice());
    let event = storage
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: DHT_KEYSPACE.to_string(),
            key: ByteView::from(key.as_bytes().as_slice()),
            value: corrupt.clone(),
            txn_id: None,
        }))
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));

    let mutation = StorageMutation::Merge {
        entries: vec![make_entry(
            1,
            key,
            unix_timestamp_secs().saturating_add(200),
        )],
    };
    assert!(
        mutate_storage_entries(&storage, 5, StorageStage::InboundPutMerge, key, &mutation,)
            .await
            .is_err()
    );

    let event = storage
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: DHT_KEYSPACE.to_string(),
            key: ByteView::from(key.as_bytes().as_slice()),
            txn_id: None,
        }))
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::ReadResult { value: Some(value), .. }) if value == corrupt
    ));
}

#[tokio::test]
async fn stale_merge_rejected() {
    let (_directory, storage) = open_storage();
    let key = DhtKeyId::from_data(b"stale-merge");
    let mutation = StorageMutation::Merge {
        entries: vec![make_entry(1, key, 0)],
    };

    assert!(matches!(
        mutate_storage_entries(&storage, 6, StorageStage::InboundPutMerge, key, &mutation).await,
        Err(DhtIoError::InvalidRequest(_))
    ));

    let event = storage
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: DHT_KEYSPACE.to_string(),
            key: ByteView::from(key.as_bytes().as_slice()),
            txn_id: None,
        }))
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::ReadResult { value: None, .. })
    ));
}

#[tokio::test]
async fn concurrent_new_keys() {
    let (_directory, storage) = open_storage();
    let locks = Arc::new(StorageLocks::default());
    let clock = DhtClock::new();
    let expires_at = unix_timestamp_secs().saturating_add(200);
    let mut tasks = tokio::task::JoinSet::new();
    for index in 1..=16u8 {
        let storage = storage.clone();
        let locks = locks.clone();
        let clock = clock.clone();
        let key = DhtKeyId::from_data(&[index; 32]);
        let mutation = StorageMutation::Merge {
            entries: vec![make_entry(index, key, expires_at)],
        };
        tasks.spawn(async move {
            mutate_storage_checked(
                &storage,
                u64::from(index),
                StorageStage::PutLocalMerge,
                key,
                &mutation,
                &clock,
                Some(&locks),
            )
            .await
        });
    }
    while let Some(result) = tasks.join_next().await {
        result
            .expect("task joins")
            .expect("concurrent put succeeds");
    }
    assert_eq!(storage.snapshot_metrics().conflicts_total, 0);
    assert_eq!(read_count(&storage).await, 16);
}

#[tokio::test]
async fn same_key_serialized() {
    let (_directory, storage) = open_storage();
    let locks = Arc::new(StorageLocks::default());
    let clock = DhtClock::new();
    let key = DhtKeyId::from_data(b"serialized-same-key");
    let expires_at = unix_timestamp_secs().saturating_add(200);
    let mut tasks = tokio::task::JoinSet::new();
    for index in 1..=8u8 {
        let storage = storage.clone();
        let locks = locks.clone();
        let clock = clock.clone();
        let mutation = StorageMutation::Merge {
            entries: vec![make_entry(index, key, expires_at)],
        };
        tasks.spawn(async move {
            mutate_storage_checked(
                &storage,
                u64::from(index),
                StorageStage::PutLocalMerge,
                key,
                &mutation,
                &clock,
                Some(&locks),
            )
            .await
        });
    }
    while let Some(result) = tasks.join_next().await {
        result
            .expect("task joins")
            .expect("serialized put succeeds");
    }
    assert_eq!(storage.snapshot_metrics().conflicts_total, 0);
    assert_eq!(read_entries(&storage, key).await.len(), 8);
    assert_eq!(read_count(&storage).await, 1);
}

#[tokio::test(start_paused = true)]
async fn inbound_bounds_lock() {
    // Waiting on a held per-key lock counts against the 8s inbound storage budget.
    let (_directory, storage) = open_storage();
    let locks = Arc::new(StorageLocks::default());
    let clock = DhtClock::new();
    let key = DhtKeyId::from_data(b"inbound-budget-key");
    let key_lock = locks.lock_for(key);
    let _held = key_lock.lock().await;
    let mutation = StorageMutation::Merge {
        entries: vec![make_entry(
            1,
            key,
            unix_timestamp_secs().saturating_add(200),
        )],
    };
    let result = run_storage_io(
        StorageStage::InboundPutMerge,
        mutate_storage_checked(
            &storage,
            99,
            StorageStage::InboundPutMerge,
            key,
            &mutation,
            &clock,
            Some(&locks),
        ),
    )
    .await;
    assert!(matches!(result, Err(DhtIoError::Timeout)));
}

#[tokio::test(start_paused = true)]
async fn lock_drops_backoff() {
    // A transaction conflict releases the per-key lock before sleeping in retry backoff.
    let (storage, receivers) = StorageHandle::new();
    let receiver = receivers.foreground;
    let locks = Arc::new(StorageLocks::default());
    let key = DhtKeyId::from_data(b"backoff-release-key");
    let mutation = StorageMutation::Merge {
        entries: vec![make_entry(
            1,
            key,
            unix_timestamp_secs().saturating_add(200),
        )],
    };
    let (conflicted_tx, conflicted_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let (_, response, ..) = receiver.recv().expect("first txn start");
        response.send(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        });
        let _ = conflicted_tx.send(());
        let _ = resume_rx.recv();
        if let Ok((_, response, ..)) = receiver.recv() {
            response.send(StorageEvent::Error {
                error: StorageError::Closed,
            });
        }
    });
    let task_locks = locks.clone();
    let task = tokio::spawn(async move {
        mutate_storage_with(
            &storage,
            100,
            StorageStage::PutLocalMerge,
            key,
            &mutation,
            None,
            Some(&task_locks),
        )
        .await
    });
    conflicted_rx.await.expect("first attempt conflicted");
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    let key_lock = locks.lock_for(key);
    let acquired = key_lock.try_lock().is_ok();
    let _ = resume_tx.send(());
    let result = task.await.expect("task joins");
    worker.join().expect("worker joins");
    assert!(result.is_err());
    assert!(
        acquired,
        "key lock must not be held during retry backoff sleep"
    );
}
