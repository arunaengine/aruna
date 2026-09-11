use super::super::*;
use super::*;

#[tokio::test]
async fn restore_document_sync_outbox_timers_schedules_drain_when_outbox_has_records() {
    let temp_dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(temp_dir.path().to_str().expect("temp path")).expect("storage opens");
    let record = crate::sync::document_sync_outbox::new_outbox_record(
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

    let restored_key = restore_document_sync_outbox_timer_and_receive_key(&storage).await;

    assert_eq!(restored_key, TaskKey::DrainDocumentSyncOutbox);
}

#[tokio::test(start_paused = true)]
async fn restore_document_sync_outbox_timers_keeps_existing_backoff_timer() {
    let _clock = freeze_clock();
    let temp_dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(temp_dir.path().to_str().expect("temp path")).expect("storage opens");
    let record = crate::sync::document_sync_outbox::new_outbox_record(
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

    restore_document_sync_outbox_timers(&storage, &task_handle).await;

    let TaskEvent::TimerScheduled { after, .. } = task_handle
        .schedule_timer_if_idle(TaskKey::DrainDocumentSyncOutbox, Duration::ZERO)
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

    drive_document_sync_outbox_drain(context.clone()).await;
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

    drive_document_sync_outbox_drain(context.clone()).await;
    assert_eq!(
        scheduled_after(&task_handle).await,
        OUTBOX_CONTINUATION_AFTER
    );
    assert!(completed.try_recv().is_err());

    drive_document_sync_outbox_drain(context).await;
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

    drive_document_sync_outbox_drain(context).await;
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
    let record = crate::sync::document_sync_outbox::new_outbox_record(
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

    drive_document_sync_outbox_drain(Arc::new(DriverContext {
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
        .schedule_timer_if_idle(TaskKey::DrainDocumentSyncOutbox, Duration::ZERO)
        .await
    else {
        panic!("expected timer schedule event");
    };
    assert!(after > Duration::from_secs(3000));
}

#[tokio::test]
async fn outbox_sync_error_retains_record_for_retry() {
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
    let record = crate::sync::document_sync_outbox::new_outbox_record(
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
        .finish_sync_drain_subbatch(
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
async fn retained_outbox_record_after_sync_failure_restores_drain_timer() {
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
    let record = crate::sync::document_sync_outbox::new_outbox_record(
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
        .finish_sync_drain_subbatch(
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

    let restored_key = restore_document_sync_outbox_timer_and_receive_key(&storage).await;
    assert_eq!(restored_key, TaskKey::DrainDocumentSyncOutbox);
}

#[tokio::test]
async fn tombstones_are_processed_before_projection_retry_return() {
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
        .finish_sync_drain_subbatch(
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
    let jobs = read_graph_prune_jobs(&storage).await;
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
        .finish_sync_drain_subbatch(
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
