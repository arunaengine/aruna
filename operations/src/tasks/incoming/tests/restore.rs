use super::harness::*;
use super::*;

#[tokio::test]
async fn restore_document_records() {
    let temp_dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(temp_dir.path().to_str().expect("temp path")).expect("storage opens");
    let record = crate::sync::document_outbox::new_outbox_record(
        node(1),
        target(),
        vec![node(2)],
        DocumentSyncOutboxEvent::Upsert {
            bytes: b"restore durable work".to_vec(),
            change: change(),
        },
        aruna_core::structs::PlacementRef::NIL,
        false,
    );
    write_outbox_record(&storage, &record).await;

    let restored_key = restore_document_key(&storage).await;

    assert_eq!(restored_key, TaskKey::DrainDocumentSyncOutbox);
}

#[tokio::test(start_paused = true)]
async fn restore_document_timer() {
    let _clock = freeze_clock();
    let temp_dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(temp_dir.path().to_str().expect("temp path")).expect("storage opens");
    let record = crate::sync::document_outbox::new_outbox_record(
        node(1),
        target(),
        vec![node(2)],
        DocumentSyncOutboxEvent::Upsert {
            bytes: b"restore durable work".to_vec(),
            change: change(),
        },
        aruna_core::structs::PlacementRef::NIL,
        false,
    );
    write_outbox_record(&storage, &record).await;

    let task_handle = TaskHandle::new();
    match task_handle
        .send_effect(Effect::Task(TaskEffect::ResetTimer {
            key: TaskKey::DrainDocumentSyncOutbox,
            after: Duration::from_secs(3600),
        }))
        .await
    {
        Event::Task(TaskEvent::TimerScheduled { .. }) => {}
        other => panic!("unexpected timer schedule event: {other:?}"),
    }

    restore_outbox_timers(&storage, &task_handle).await;

    let TaskEvent::TimerScheduled { after, .. } = task_handle
        .schedule_idle_timer(TaskKey::DrainDocumentSyncOutbox, Duration::ZERO)
        .await
    else {
        panic!("expected timer schedule event");
    };
    assert_eq!(
        after,
        Duration::from_secs(3600),
        "durable rearm must preserve the active backoff deadline"
    );
}

#[tokio::test]
async fn installed_fence() {
    let _clock = freeze_clock();
    let InstalledHarness {
        _dir,
        task_handle,
        context,
        handler,
        mut completed,
        net,
        ..
    } = installed_setup().await;

    drive_sync_drain(context.clone()).await;
    assert!(recv_progress(&mut completed).await);
    {
        let rotation = handler.rotation.lock().expect("rotation lock");
        assert_eq!(rotation.totals.examined, 1);
        assert!(rotation.cursor.is_some());
        assert_eq!(rotation.continuations, 1);
    }
    assert_eq!(
        scheduled_after(&task_handle).await,
        OUTBOX_CONTINUATION_AFTER
    );
    assert!(completed.try_recv().is_err());

    drive_sync_drain(context.clone()).await;
    assert_eq!(
        scheduled_after(&task_handle).await,
        OUTBOX_CONTINUATION_AFTER
    );
    assert!(completed.try_recv().is_err());

    drive_sync_drain(context).await;
    assert_eq!(
        scheduled_after(&task_handle).await,
        OUTBOX_CONTINUATION_AFTER
    );
    assert!(completed.try_recv().is_err());

    shutdown_net(&net).await;
}

#[tokio::test]
async fn installed_continues() {
    let _clock = freeze_clock();
    let InstalledHarness {
        _dir,
        storage,
        net,
        context,
        mut completed,
        ..
    } = installed_setup().await;

    drive_sync_drain(context).await;
    assert!(recv_progress(&mut completed).await);
    // Tokio rounds a timer deadline up to the next millisecond, so the clock
    // has to pass the interval rather than land exactly on it.
    tokio::time::advance(OUTBOX_CONTINUATION_AFTER + Duration::from_millis(1)).await;
    assert!(recv_progress(&mut completed).await);
    assert!(completed.try_recv().is_err());
    assert!(
        read_outbox_records(&storage, &[], None, 4)
            .await
            .expect("read drained records")
            .records
            .is_empty()
    );

    shutdown_net(&net).await;
}

#[tokio::test]
async fn drain_keeps_timer() {
    let temp_dir = tempdir().expect("temp dir");
    let storage = FjallStorage::open(temp_dir.path().to_str().expect("storage opens"))
        .expect("storage opens");
    let record = crate::sync::document_outbox::new_outbox_record(
        node(1),
        target(),
        vec![node(2)],
        DocumentSyncOutboxEvent::Upsert {
            bytes: b"direct fence".to_vec(),
            change: change(),
        },
        aruna_core::structs::PlacementRef::NIL,
        false,
    );
    write_outbox_record(&storage, &record).await;

    let task_handle = TaskHandle::new();
    let (seen_tx, mut seen_rx) = mpsc::channel(1);
    task_handle
        .set_inbound_handler(Arc::new(RecordingTaskHandler { seen: seen_tx }))
        .await;
    match task_handle
        .send_effect(Effect::Task(TaskEffect::ResetTimer {
            key: TaskKey::DrainDocumentSyncOutbox,
            after: Duration::from_secs(3600),
        }))
        .await
    {
        Event::Task(TaskEvent::TimerScheduled { .. }) => {}
        other => panic!("unexpected timer schedule event: {other:?}"),
    }

    drive_sync_drain(Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: Some(task_handle.clone()),
        compute_handle: None,
    }))
    .await;

    assert!(
        tokio::time::timeout(Duration::from_millis(50), seen_rx.recv())
            .await
            .is_err(),
        "the direct fence must not replace the active timer"
    );
    let TaskEvent::TimerScheduled { after, .. } = task_handle
        .schedule_idle_timer(TaskKey::DrainDocumentSyncOutbox, Duration::ZERO)
        .await
    else {
        panic!("expected timer schedule event");
    };
    assert!(after > Duration::from_secs(3000));
}

#[tokio::test]
async fn outbox_sync_retry() {
    let temp_dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(temp_dir.path().to_str().expect("temp path")).expect("storage opens");
    let context = Arc::new(DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let handler = OperationsTaskHandler::new(context, JobsRuntime::new());
    let record = crate::sync::document_outbox::new_outbox_record(
        node(1),
        target(),
        vec![node(2)],
        DocumentSyncOutboxEvent::Upsert {
            bytes: b"retained work".to_vec(),
            change: change(),
        },
        aruna_core::structs::PlacementRef::NIL,
        false,
    );
    let key = outbox_key(&record).to_vec();

    write_outbox_record(&storage, &record).await;

    let outcome = handler
        .finish_sync_batch(
            &TaskKey::DrainDocumentSyncOutbox,
            vec![key.clone()],
            Vec::new(),
            Event::Net(NetEvent::DocumentSync(DocumentSyncNetEvent::Error {
                target: Some(record.target.clone()),
                error: "only 1/2 peers synced".to_string(),
            })),
            DrainSyncOutcome::default(),
        )
        .await;

    assert!(outcome.retry_needed);
    let retained = read_outbox_record(&storage, &key)
        .await
        .expect("outbox record reads");
    assert_eq!(retained, Some(record));
}

#[tokio::test]
async fn retained_outbox_timer() {
    let temp_dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(temp_dir.path().to_str().expect("temp path")).expect("storage opens");
    let context = Arc::new(DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let handler = OperationsTaskHandler::new(context, JobsRuntime::new());
    let record = crate::sync::document_outbox::new_outbox_record(
        node(1),
        target(),
        vec![node(2)],
        DocumentSyncOutboxEvent::Upsert {
            bytes: b"retry after restart".to_vec(),
            change: change(),
        },
        aruna_core::structs::PlacementRef::NIL,
        false,
    );
    let key = outbox_key(&record).to_vec();
    write_outbox_record(&storage, &record).await;

    let outcome = handler
        .finish_sync_batch(
            &TaskKey::DrainDocumentSyncOutbox,
            vec![key.clone()],
            Vec::new(),
            Event::Net(NetEvent::DocumentSync(DocumentSyncNetEvent::Error {
                target: Some(record.target.clone()),
                error: "sync failed before all peers acknowledged".to_string(),
            })),
            DrainSyncOutcome::default(),
        )
        .await;
    assert!(outcome.retry_needed);
    assert_eq!(
        read_outbox_record(&storage, &key)
            .await
            .expect("outbox record reads"),
        Some(record)
    );

    let restored_key = restore_document_key(&storage).await;
    assert_eq!(restored_key, TaskKey::DrainDocumentSyncOutbox);
}

#[tokio::test]
async fn tombstones_are_return() {
    let temp_dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(temp_dir.path().to_str().expect("temp path")).expect("storage opens");
    let context = Arc::new(DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let handler = OperationsTaskHandler::new(context, JobsRuntime::new());
    let document_id = Ulid::from_parts(17, 1);
    let tombstone = MetadataGraphLifecycleRecord::deleted(
        "urn:graph:tombstone-before-retry".to_string(),
        RealmId::from_bytes([3; 32]),
        Ulid::from_parts(18, 1),
        document_id,
        19,
    );

    let outcome = handler
        .finish_sync_batch(
            &TaskKey::DrainDocumentSyncOutbox,
            Vec::new(),
            Vec::new(),
            Event::Net(NetEvent::DocumentSync(
                DocumentSyncNetEvent::DocumentsReconciled {
                    applied: 1,
                    targets: vec![DocumentSyncTarget::MetadataCreateEvent {
                        document_id,
                        event_id: Ulid::from_parts(20, 1),
                    }],
                    metadata_create_events: Vec::new(),
                    metadata_graph_tombstones: vec![tombstone.clone()],
                },
            )),
            DrainSyncOutcome::default(),
        )
        .await;

    assert!(outcome.retry_needed);
    let jobs = read_graph_jobs(&storage).await;
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].graph_iri, tombstone.graph_iri);
}

#[tokio::test]
async fn drain_reconcile_wakes() {
    let temp_dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(temp_dir.path().to_str().expect("temp path")).expect("storage opens");
    let realm_id = RealmId::from_bytes([44u8; 32]);
    let net = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().expect("valid bind addr"),
            realm_id,
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        storage.clone(),
    )
    .await
    .expect("net handle");
    let mut revisions = net.subscribe_dashboard_changes();
    let context = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: Some(net.clone()),
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let handler = OperationsTaskHandler::new(context, JobsRuntime::new());

    let outcome = handler
        .finish_sync_batch(
            &TaskKey::DrainDocumentSyncOutbox,
            Vec::new(),
            vec![DocumentSyncTarget::RealmConfig { realm_id }],
            Event::Net(NetEvent::DocumentSync(
                DocumentSyncNetEvent::DocumentsReconciled {
                    applied: 0,
                    targets: Vec::new(),
                    metadata_create_events: Vec::new(),
                    metadata_graph_tombstones: Vec::new(),
                },
            )),
            DrainSyncOutcome::default(),
        )
        .await;

    assert!(!outcome.retry_needed);
    tokio::time::timeout(Duration::from_secs(2), revisions.changed())
        .await
        .expect("dashboard revision arrives")
        .expect("channel open");
    assert_eq!(*revisions.borrow_and_update(), 1);
    net.shutdown().await;
}

#[tokio::test]
async fn drain_reconcile_summary() {
    use aruna_core::keyspaces::{USAGE_NODE_STATS_KEYSPACE, USAGE_STATS_KEYSPACE};
    use aruna_core::structs::{
        NODE_USAGE_SUMMARY_GLOBAL_KEY, NodeUsageSnapshot, UsageCounters, global_shard_key,
        usage_global_key,
    };
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};

    async fn write_stat(
        storage: &aruna_storage::StorageHandle,
        key_space: &str,
        key: Vec<u8>,
        value: Vec<u8>,
    ) {
        match storage
            .send_effect(Effect::Storage(StorageEffect::Write {
                key_space: key_space.to_string(),
                key: key.into(),
                value: value.into(),
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected write event: {other:?}"),
        }
    }

    let temp_dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(temp_dir.path().to_str().expect("temp path")).expect("storage opens");
    let realm_id = RealmId::from_bytes([44u8; 32]);
    let net = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().expect("valid bind addr"),
            realm_id,
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        storage.clone(),
    )
    .await
    .expect("net handle");
    let remote = node(2);

    // Local global counters (10) plus a remote node's snapshot (5) should sum
    // to 15 in the refreshed realm summary cache.
    write_stat(
        &storage,
        USAGE_STATS_KEYSPACE,
        global_shard_key(0),
        UsageCounters {
            logical_bytes: 10,
            ..Default::default()
        }
        .to_bytes()
        .unwrap(),
    )
    .await;
    write_stat(
        &storage,
        USAGE_NODE_STATS_KEYSPACE,
        usage_global_key(remote),
        NodeUsageSnapshot {
            node_id: remote,
            counters: UsageCounters {
                logical_bytes: 5,
                ..Default::default()
            },
        }
        .to_bytes()
        .unwrap(),
    )
    .await;

    let context = Arc::new(DriverContext {
        storage_handle: storage.clone(),
        net_handle: Some(net.clone()),
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let handler = OperationsTaskHandler::new(context, JobsRuntime::new());

    let outcome = handler
        .finish_sync_batch(
            &TaskKey::DrainDocumentSyncOutbox,
            Vec::new(),
            Vec::new(),
            Event::Net(NetEvent::DocumentSync(
                DocumentSyncNetEvent::DocumentsReconciled {
                    applied: 1,
                    targets: vec![DocumentSyncTarget::NodeUsage {
                        realm_id,
                        node_id: remote,
                        group_id: None,
                    }],
                    metadata_create_events: Vec::new(),
                    metadata_graph_tombstones: Vec::new(),
                },
            )),
            DrainSyncOutcome::default(),
        )
        .await;

    assert!(!outcome.retry_needed);
    let summary = match storage
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: USAGE_NODE_STATS_KEYSPACE.to_string(),
            key: NODE_USAGE_SUMMARY_GLOBAL_KEY.to_vec().into(),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
        other => panic!("unexpected read event: {other:?}"),
    };
    let summary = summary.expect("realm usage summary refreshed by drain reconcile");
    assert_eq!(
        UsageCounters::from_bytes(summary.as_ref())
            .unwrap()
            .logical_bytes,
        15
    );

    net.shutdown().await;
}

#[tokio::test]
async fn drain_reconcile_config() {
    use aruna_core::keyspaces::USAGE_NODE_STATS_KEYSPACE;
    use aruna_core::structs::{NODE_USAGE_SUMMARY_GLOBAL_KEY, UsageCounters};
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};

    let temp_dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(temp_dir.path().to_str().expect("temp path")).expect("storage opens");
    let realm_id = RealmId::from_bytes([45u8; 32]);
    let net = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().expect("valid bind addr"),
            realm_id,
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        storage.clone(),
    )
    .await
    .expect("net handle");
    match storage
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: USAGE_NODE_STATS_KEYSPACE.to_string(),
            key: NODE_USAGE_SUMMARY_GLOBAL_KEY.to_vec().into(),
            value: UsageCounters {
                logical_bytes: 99,
                ..Default::default()
            }
            .to_bytes()
            .unwrap()
            .into(),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected write event: {other:?}"),
    }

    let context = Arc::new(DriverContext {
        storage_handle: storage.clone(),
        net_handle: Some(net.clone()),
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let handler = OperationsTaskHandler::new(context, JobsRuntime::new());

    let outcome = handler
        .finish_sync_batch(
            &TaskKey::DrainDocumentSyncOutbox,
            Vec::new(),
            vec![DocumentSyncTarget::RealmConfig { realm_id }],
            Event::Net(NetEvent::DocumentSync(
                DocumentSyncNetEvent::DocumentsReconciled {
                    applied: 0,
                    targets: Vec::new(),
                    metadata_create_events: Vec::new(),
                    metadata_graph_tombstones: Vec::new(),
                },
            )),
            DrainSyncOutcome::default(),
        )
        .await;

    assert!(!outcome.retry_needed);
    match storage
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: USAGE_NODE_STATS_KEYSPACE.to_string(),
            key: NODE_USAGE_SUMMARY_GLOBAL_KEY.to_vec().into(),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {}
        other => panic!("expected summary cache to be cleared, got {other:?}"),
    }

    net.shutdown().await;
}

async fn restore_document_key(storage: &aruna_storage::StorageHandle) -> TaskKey {
    let task_handle = TaskHandle::new();
    let (seen_tx, mut seen_rx) = mpsc::channel(1);
    task_handle
        .set_inbound_handler(Arc::new(RecordingTaskHandler { seen: seen_tx }))
        .await;

    restore_outbox_timers(storage, &task_handle).await;

    tokio::time::timeout(Duration::from_secs(1), seen_rx.recv())
        .await
        .expect("restored drain timer should fire")
        .expect("recording handler should receive timer key")
}

#[tokio::test]
async fn placement_storage_rearms() {
    let realm_id = RealmId::from_bytes([43u8; 32]);
    let node_id = node(7);
    let key = TaskKey::SyncPlacements { realm_id, node_id };
    let temp_dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(temp_dir.path().to_str().expect("temp path")).expect("storage opens");
    match storage
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: ByteView::from(realm_id.as_bytes().to_vec()),
            value: ByteView::from(b"malformed config".to_vec()),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected realm config write: {other:?}"),
    }

    let task_handle = TaskHandle::new();
    let context = Arc::new(DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: Some(task_handle),
        compute_handle: None,
    });

    let before_ms = unix_timestamp_millis();
    OperationsTaskHandler::new(context, JobsRuntime::new())
        .handle_timer(key.clone())
        .await;
    let after_ms = unix_timestamp_millis();

    let persisted = match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: TASK_TIMER_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: 2,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => values,
        other => panic!("unexpected timer iter result: {other:?}"),
    };
    assert_eq!(persisted.len(), 1, "one retry timer must be persisted");
    let timer: aruna_core::task::PersistedTaskTimer =
        postcard::from_bytes(&persisted[0].1).expect("persisted timer decodes");
    assert_eq!(timer.key, key);
    let retry_ms = crate::sync::shard_placement::SYNC_PLACEMENT_RETRY_AFTER.as_millis() as u64;
    assert!(timer.due_at_unix_millis >= before_ms.saturating_add(retry_ms));
    assert!(timer.due_at_unix_millis <= after_ms.saturating_add(retry_ms));
}
