use super::harness::*;
use super::*;

#[test]
fn admin_block_spans() {
    let origin = node(4);
    let blocked_topic = irokle::TopicId::hash(b"blocked-admin-page");
    let healthy_topic = irokle::TopicId::hash(b"healthy-admin-page");
    let mut defer = DrainDeferState::default();
    let (_, deferred, _) = partition_drain_records(
        vec![(b"first".to_vec(), admin_record(origin, 1), blocked_topic)],
        &mut defer,
        true,
        |_| false,
        |_| DeferOutcome::Retry,
    );
    assert_eq!(deferred.len(), 1);

    let (published, deferred, undeliverable) = partition_drain_records(
        vec![(b"second".to_vec(), admin_record(origin, 2), healthy_topic)],
        &mut defer,
        true,
        |_| true,
        |_| DeferOutcome::Retry,
    );
    assert!(published.is_empty());
    assert_eq!(deferred.len(), 1);
    assert!(undeliverable.is_empty());
}

// A blocked admin operation blocks the rest of its origin sequence for the
// whole rotation, whatever topic the later records ride: publishing a later
// origin_seq first would drop the earlier one as StaleOriginSequence.
#[test]
fn admin_origin_blocks() {
    let origin = node(4);
    let blocked_topic = irokle::TopicId::hash(b"blocked-admin-topic");
    let healthy_topic = irokle::TopicId::hash(b"healthy-admin-topic");
    let records = vec![
        (b"first".to_vec(), admin_record(origin, 1), blocked_topic),
        (b"second".to_vec(), admin_record(origin, 2), healthy_topic),
    ];

    let mut defer = DrainDeferState::default();
    let (to_publish, deferred, undeliverable) = partition_drain_records(
        records,
        &mut defer,
        true,
        |topic| topic != blocked_topic,
        |_| DeferOutcome::Retry,
    );

    assert!(
        to_publish.is_empty(),
        "a later origin_seq must not overtake"
    );
    assert_eq!(deferred.len(), 2);
    assert!(undeliverable.is_empty());
    assert!(defer.blocked_origins.contains(&origin));
}

#[test]
fn topic_block_spans() {
    let topic = irokle::TopicId::hash(b"blocked-topic-pages");
    let healthy_topic = irokle::TopicId::hash(b"healthy-topic-pages");
    let mut defer = DrainDeferState::default();
    let (_, deferred, _) = partition_drain_records(
        vec![(b"first".to_vec(), shard_topic_record(1), topic)],
        &mut defer,
        true,
        |_| false,
        |_| DeferOutcome::Retry,
    );
    assert_eq!(deferred.len(), 1);

    let (published, deferred, undeliverable) = partition_drain_records(
        vec![
            (b"healthy".to_vec(), shard_topic_record(3), healthy_topic),
            (b"second".to_vec(), shard_topic_record(2), topic),
        ],
        &mut defer,
        true,
        |_| true,
        |_| DeferOutcome::Retry,
    );
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].0, b"healthy".to_vec());
    assert_eq!(deferred.len(), 1);
    assert!(undeliverable.is_empty());
}

// Two FIFO-adjacent records for one shard topic must never split across a
// defer/publish boundary, or a between-records availability flip would publish the
// newer first and invert origin sequence. Availability is evaluated once per topic.
#[test]
fn drain_partition_never_splits_a_topic_when_availability_flips() {
    let topic = irokle::TopicId::hash(b"shard-genesis-race");
    let older = shard_topic_record(1);
    let newer = shard_topic_record(2);
    assert!(older.target.uses_shard_topic());
    let records = vec![
        (b"older".to_vec(), older, topic),
        (b"newer".to_vec(), newer, topic),
    ];

    let mut calls = 0usize;
    let mut defer = DrainDeferState::default();
    let (to_publish, deferred, undeliverable) = partition_drain_records(
        records,
        &mut defer,
        true,
        |_| {
            calls += 1;
            calls > 1
        },
        |_| DeferOutcome::Retry,
    );

    assert_eq!(calls, 1, "topic availability is evaluated once per run");
    assert!(
        to_publish.is_empty(),
        "no record of a deferred topic may publish"
    );
    assert_eq!(deferred.len(), 2);
    assert!(undeliverable.is_empty());
}

#[test]
fn outbox_upsert_maps_to_publish_with_revision() {
    let event_id = Ulid::from_parts(10, 1);
    let target = target();
    let change = change();
    let publish = document_publish_from_outbox(
        event_id,
        target.clone(),
        DocumentSyncOutboxEvent::Upsert {
            bytes: vec![1, 2, 3],
            change,
        },
        aruna_core::structs::PlacementRef::NIL,
        true,
    );

    assert_eq!(publish.target(), &target);
    assert_eq!(publish.event_id(), event_id);
    assert!(publish.allow_genesis());
    assert!(matches!(
        publish,
        DocumentSyncPublish::Upsert { bytes, change: actual, .. }
            if bytes == vec![1, 2, 3] && actual == change
    ));
}

#[test]
fn partial_publish_indices_select_exact_outbox_records() {
    let duplicate_target = target();
    let other_target = DocumentSyncTarget::Group {
        group_id: Ulid::from_parts(7, 2),
    };
    let subbatch = DrainSubBatch {
        peers: vec![node(2)],
        documents: Vec::new(),
        topics: vec![
            irokle::TopicId::hash(b"first"),
            irokle::TopicId::hash(b"second"),
            irokle::TopicId::hash(b"third"),
        ],
        origins: vec![None, None, Some(node(3))],
        targets: vec![
            duplicate_target.clone(),
            other_target,
            duplicate_target.clone(),
        ],
        record_keys: vec![b"first".to_vec(), b"second".to_vec(), b"third".to_vec()],
    };

    let selected = subbatch
        .sync_subset(&[2])
        .expect("published index selects a record");

    assert_eq!(selected.targets, vec![duplicate_target]);
    assert_eq!(selected.topics, vec![irokle::TopicId::hash(b"third")]);
    assert_eq!(selected.origins, vec![Some(node(3))]);
    assert_eq!(selected.record_keys, vec![b"third".to_vec()]);
    assert!(selected.documents.is_empty());
    assert!(subbatch.sync_subset(&[3]).is_none());
}

#[tokio::test]
async fn topic_page_blocks() {
    let _clock = freeze_clock();
    let realm_id = RealmId::from_bytes([49u8; 32]);
    let dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
    let net = make_net_handle(realm_id, &storage, [49u8; 32]).await;
    tokio::time::pause();
    let task_handle = TaskHandle::new();
    let context = Arc::new(DriverContext {
        storage_handle: storage.clone(),
        net_handle: Some(net.clone()),
        blob_handle: None,
        metadata_handle: None,
        task_handle: Some(task_handle.clone()),
        compute_handle: None,
    });
    let blocked_target = DocumentSyncTarget::Group {
        group_id: Ulid::from_parts(10, 1),
    };
    let healthy_target = DocumentSyncTarget::Group {
        group_id: Ulid::from_parts(10, 2),
    };
    // A shard topic keys on (strategy, shard) alone, so the two records need
    // different placements to ride a blocked and a healthy topic.
    let blocked_change = shard_change(49);
    let healthy_change = shard_change(51);
    let blocked_topic = blocked_target.sync_topic_id(realm_id, &blocked_change.placement);
    let healthy_topic = healthy_target.sync_topic_id(realm_id, &healthy_change.placement);
    net.ensure_document_sync_topics(&[healthy_topic], Vec::new())
        .expect("healthy topic genesis");
    let blocked = crate::sync::document_outbox::new_outbox_record_with_id(
        Ulid::from_parts(1, 1),
        node(1),
        blocked_target,
        Vec::new(),
        DocumentSyncOutboxEvent::Upsert {
            bytes: b"blocked".to_vec(),
            change: blocked_change,
        },
        aruna_core::structs::PlacementRef::NIL,
        true,
    );
    let healthy = crate::sync::document_outbox::new_outbox_record_with_id(
        Ulid::from_parts(1, 2),
        node(1),
        healthy_target,
        Vec::new(),
        DocumentSyncOutboxEvent::Upsert {
            bytes: b"healthy".to_vec(),
            change: healthy_change,
        },
        aruna_core::structs::PlacementRef::NIL,
        true,
    );
    let blocked_key = outbox_key(&blocked).to_vec();
    let healthy_key = outbox_key(&healthy).to_vec();
    write_outbox_record(&storage, &blocked).await;
    write_outbox_record(&storage, &healthy).await;
    let handler =
        OperationsTaskHandler::new(context, JobsRuntime::new()).with_outbox_limits(1, 1, 2);

    handler.drain_document_sync_outbox().await;
    assert_eq!(
        read_outbox_record(&storage, &blocked_key)
            .await
            .expect("read blocked record"),
        Some(blocked.clone())
    );
    assert_eq!(
        scheduled_after(&task_handle).await,
        OUTBOX_CONTINUATION_AFTER
    );
    handler.drain_document_sync_outbox().await;
    assert_eq!(
        read_outbox_record(&storage, &healthy_key)
            .await
            .expect("read healthy record"),
        None,
        "a later page may progress while the blocked topic is retained"
    );
    net.ensure_document_sync_topics(&[blocked_topic], Vec::new())
        .expect("blocked topic genesis");
    for _ in 0..3 {
        handler.drain_document_sync_outbox().await;
    }
    assert_eq!(
        read_outbox_record(&storage, &blocked_key)
            .await
            .expect("read retried record"),
        None
    );
    shutdown_net(&net).await;
}

#[tokio::test]
async fn admin_page_blocks() {
    let _clock = freeze_clock();
    let realm_id = RealmId::from_bytes([50u8; 32]);
    let dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
    let net = make_net_handle(realm_id, &storage, [50u8; 32]).await;
    tokio::time::pause();
    let task_handle = TaskHandle::new();
    let context = Arc::new(DriverContext {
        storage_handle: storage.clone(),
        net_handle: Some(net.clone()),
        blob_handle: None,
        metadata_handle: None,
        task_handle: Some(task_handle.clone()),
        compute_handle: None,
    });
    // The local node is the origin: it can only publish envelopes it signs.
    let origin = net.node_id();
    let blocked_target = DocumentSyncTarget::Group {
        group_id: Ulid::from_parts(11, 1),
    };
    let healthy_target = DocumentSyncTarget::Group {
        group_id: Ulid::from_parts(11, 2),
    };
    // A shard topic keys on (strategy, shard) alone, so the two records need
    // different placements to ride a blocked and a healthy topic.
    let blocked_placement = admin_placement(1);
    let healthy_placement = admin_placement(2);
    let blocked_topic = blocked_target.sync_topic_id(realm_id, &blocked_placement);
    let healthy_topic = healthy_target.sync_topic_id(realm_id, &healthy_placement);
    net.ensure_document_sync_topics(&[healthy_topic], Vec::new())
        .expect("healthy topic genesis");
    let blocked = admin_outbox(realm_id, origin, 1, blocked_target, blocked_placement);
    let healthy = admin_outbox(realm_id, origin, 2, healthy_target, healthy_placement);
    let blocked_key = outbox_key(&blocked).to_vec();
    let healthy_key = outbox_key(&healthy).to_vec();
    write_outbox_record(&storage, &blocked).await;
    write_outbox_record(&storage, &healthy).await;
    let handler =
        OperationsTaskHandler::new(context, JobsRuntime::new()).with_outbox_limits(1, 1, 2);

    handler.drain_document_sync_outbox().await;
    assert_eq!(
        scheduled_after(&task_handle).await,
        OUTBOX_CONTINUATION_AFTER
    );
    handler.drain_document_sync_outbox().await;
    assert_eq!(
        read_outbox_record(&storage, &healthy_key)
            .await
            .expect("read blocked-origin record"),
        Some(healthy.clone()),
        "a later origin sequence must remain blocked across pages"
    );
    net.ensure_document_sync_topics(&[blocked_topic], Vec::new())
        .expect("blocked topic genesis");
    for _ in 0..3 {
        handler.drain_document_sync_outbox().await;
    }
    assert_eq!(
        read_outbox_record(&storage, &blocked_key)
            .await
            .expect("read admin retry"),
        None
    );
    assert_eq!(
        read_outbox_record(&storage, &healthy_key)
            .await
            .expect("read admin suffix"),
        None
    );
    shutdown_net(&net).await;
}

// Closing a rotation returns its accumulated totals and clears every
// ordering block, so the next rotation starts clean at the head.
#[test]
fn rotation_close_clears() {
    let mut rotation = OutboxRotation {
        cursor: Some(b"somewhere".to_vec()),
        continuations: 3,
        ..OutboxRotation::default()
    };
    rotation
        .blocked_topics
        .insert(irokle::TopicId::hash(b"blocked"));
    rotation.blocked_origins.insert(node(4));
    rotation
        .undeliverable_topics
        .insert(irokle::TopicId::hash(b"undeliverable"));
    rotation.totals.examined = 12;
    rotation.totals.deleted = 5;
    rotation.totals.invocations = 2;

    let totals = rotation.close();

    assert_eq!(totals.examined, 12);
    assert_eq!(totals.deleted, 5);
    assert_eq!(totals.invocations, 2);
    assert!(rotation.cursor.is_none());
    assert!(rotation.blocked_topics.is_empty());
    assert!(rotation.blocked_origins.is_empty());
    assert!(rotation.undeliverable_topics.is_empty());
    assert_eq!(rotation.continuations, 0);
    assert_eq!(rotation.totals.examined, 0);
}

#[test]
fn rotation_holds_boundary() {
    let mut rotation = OutboxRotation {
        boundary: Some(b"b".to_vec()),
        cursor: Some(b"a".to_vec()),
        ..OutboxRotation::default()
    };

    assert!(rotation.admits(b"a"));
    assert!(rotation.admits(b"b"));
    assert!(!rotation.admits(b"c"));
    assert!(!rotation.at_end(Some(b"a")));
    assert!(rotation.at_end(Some(b"b")));

    rotation.close();
    assert!(rotation.boundary.is_none());
    assert!(rotation.admits(b"appended"));
}

#[tokio::test(start_paused = true)]
async fn close_routes_defer() {
    let _clock = freeze_clock();
    let (_dir, handler, task_handle) = outbox_handler();
    let key = TaskKey::DrainDocumentSyncOutbox;
    handler
        .retry_backoff
        .lock()
        .expect("retry backoff mutex poisoned")
        .insert(key.clone(), 3);
    let rotation = OutboxRotation {
        totals: RotationTotals {
            deferred: 2,
            deleted: 1,
            invocations: 2,
            ..RotationTotals::default()
        },
        ..OutboxRotation::default()
    };

    handler.close_rotation(key.clone(), rotation).await;

    assert_eq!(
        scheduled_after(&task_handle).await,
        DOCUMENT_SYNC_DEFER_RETRY_AFTER
    );
    assert!(
        !handler
            .retry_backoff
            .lock()
            .expect("retry backoff mutex poisoned")
            .contains_key(&key),
        "progress in the clean suffix resets failure backoff"
    );
}

#[tokio::test(start_paused = true)]
async fn close_keeps_retry() {
    let _clock = freeze_clock();
    let (_dir, handler, task_handle) = outbox_handler();
    let key = TaskKey::DrainDocumentSyncOutbox;
    handler
        .retry_backoff
        .lock()
        .expect("retry backoff mutex poisoned")
        .insert(key.clone(), 3);
    let rotation = OutboxRotation {
        totals: RotationTotals {
            retry_invocations: 1,
            invocations: 2,
            ..RotationTotals::default()
        },
        ..OutboxRotation::default()
    };

    handler.close_rotation(key.clone(), rotation).await;

    assert_eq!(scheduled_after(&task_handle).await, Duration::from_secs(2));
    assert_eq!(
        handler
            .retry_backoff
            .lock()
            .expect("retry backoff mutex poisoned")
            .get(&key),
        Some(&4)
    );
}

#[tokio::test(start_paused = true)]
async fn close_resets_progress() {
    let _clock = freeze_clock();
    let (_dir, handler, task_handle) = outbox_handler();
    let key = TaskKey::DrainDocumentSyncOutbox;
    handler
        .retry_backoff
        .lock()
        .expect("retry backoff mutex poisoned")
        .insert(key.clone(), 3);
    let rotation = OutboxRotation {
        totals: RotationTotals {
            // A retry on an early page followed by a clean suffix made
            // progress, so the next retry returns to the base interval.
            deleted: 1,
            retry_invocations: 1,
            invocations: 2,
            ..RotationTotals::default()
        },
        ..OutboxRotation::default()
    };

    handler.close_rotation(key.clone(), rotation).await;

    assert_eq!(
        scheduled_after(&task_handle).await,
        Duration::from_millis(250)
    );
    assert_eq!(
        handler
            .retry_backoff
            .lock()
            .expect("retry backoff mutex poisoned")
            .get(&key),
        Some(&1)
    );
}

#[tokio::test(start_paused = true)]
async fn retry_suffix_closes() {
    let _clock = freeze_clock();
    let (_dir, handler, task_handle) = outbox_handler();
    let key = TaskKey::DrainDocumentSyncOutbox;
    // The first page retried; a clean suffix made progress before the
    // rotation reached its observed high-water boundary.
    let rotation = OutboxRotation {
        boundary: Some(b"last".to_vec()),
        cursor: Some(b"first".to_vec()),
        totals: RotationTotals {
            deleted: 1,
            retry_invocations: 1,
            invocations: 2,
            ..RotationTotals::default()
        },
        ..OutboxRotation::default()
    };

    handler.finish_retry(key.clone(), rotation, true).await;

    assert_eq!(
        scheduled_after(&task_handle).await,
        Duration::from_millis(250)
    );
    let rotation = handler.rotation.lock().expect("rotation lock");
    assert!(rotation.boundary.is_none());
    assert!(rotation.cursor.is_none());
    assert_eq!(
        handler
            .retry_backoff
            .lock()
            .expect("retry backoff mutex poisoned")
            .get(&key),
        Some(&1)
    );
}

#[tokio::test(start_paused = true)]
async fn midpoint_retry_keeps() {
    let _clock = freeze_clock();
    let (_dir, handler, task_handle) = outbox_handler();
    let key = TaskKey::DrainDocumentSyncOutbox;
    let topic = irokle::TopicId::hash(b"midpoint-retry-topic");
    handler
        .retry_backoff
        .lock()
        .expect("retry backoff mutex poisoned")
        .insert(key.clone(), 3);
    let mut rotation = OutboxRotation {
        boundary: Some(b"last".to_vec()),
        cursor: Some(b"middle".to_vec()),
        totals: RotationTotals {
            retry_invocations: 1,
            invocations: 1,
            ..RotationTotals::default()
        },
        ..OutboxRotation::default()
    };
    rotation.blocked_topics.insert(topic);

    handler.finish_retry(key.clone(), rotation, false).await;

    {
        let rotation = handler.rotation.lock().expect("rotation lock");
        assert_eq!(rotation.cursor.as_deref(), Some(b"middle".as_slice()));
        assert!(rotation.blocked_topics.contains(&topic));
        assert_eq!(rotation.totals.retry_invocations, 1);
    }
    assert_eq!(scheduled_after(&task_handle).await, Duration::from_secs(2));
}

#[tokio::test]
async fn config_reloads_between() {
    let ConfigHarness {
        _dir,
        storage,
        net,
        handler,
        mut config,
        realm_id,
        placement,
        shard_target,
    } = config_setup().await;
    handler.drain_document_sync_outbox().await;
    {
        let rotation = handler.rotation.lock().expect("rotation lock");
        assert!(rotation.cursor.is_some());
        assert_eq!(rotation.totals.deleted, 1);
    }

    config.ensure_node(net.node_id(), RealmNodeKind::Server);
    write_realm_config(&storage, realm_id, &config, net.node_id()).await;
    let shard_topic = shard_target.sync_topic_id(realm_id, &placement);
    net.ensure_document_sync_topics(&[shard_topic], Vec::new())
        .expect("updated holder topic genesis");

    handler.drain_document_sync_outbox().await;
    assert!(
        read_outbox_records(&storage, &[], None, 4)
            .await
            .expect("read revalidated records")
            .records
            .is_empty(),
        "the continuation must use the updated holder config"
    );

    net.shutdown().await;
}

// The invocation bound is a work-unit limit, never a latency assertion.
#[test]
fn outbox_bound_finite() {
    assert_eq!(
        OUTBOX_INVOCATION_RECORDS,
        OUTBOX_INVOCATION_PAGES * OUTBOX_DRAIN_BATCH_SIZE
    );
    assert_ne!(OUTBOX_INVOCATION_RECORDS, 0);
}

#[test]
fn drain_partition_publishes_all_records_of_an_available_topic_in_fifo_order() {
    let topic = irokle::TopicId::hash(b"shard-genesis-present");
    let records = vec![
        (b"older".to_vec(), shard_topic_record(1), topic),
        (b"newer".to_vec(), shard_topic_record(2), topic),
    ];

    let mut defer = DrainDeferState::default();
    let (to_publish, deferred, undeliverable) =
        partition_drain_records(records, &mut defer, true, |_| true, |_| DeferOutcome::Retry);

    assert!(deferred.is_empty());
    assert!(undeliverable.is_empty());
    let keys: Vec<Vec<u8>> = to_publish.into_iter().map(|(key, _, _)| key).collect();
    assert_eq!(keys, vec![b"older".to_vec(), b"newer".to_vec()]);
}

// A record for a bucket this node does not hold can never publish: it may
// neither mint the topic's genesis nor join the topic. Deferring it forever
// would be silent data loss, so it is separated out to be dropped loudly.
#[test]
fn unheld_bucket_records_are_undeliverable() {
    let topic = irokle::TopicId::hash(b"unheld-bucket");
    let records = vec![
        (b"older".to_vec(), shard_topic_record(1), topic),
        (b"newer".to_vec(), shard_topic_record(2), topic),
    ];

    let mut classified = 0usize;
    let mut defer = DrainDeferState::default();
    let (to_publish, deferred, undeliverable) = partition_drain_records(
        records,
        &mut defer,
        true,
        |_| false,
        |_| {
            classified += 1;
            DeferOutcome::Undeliverable
        },
    );

    assert_eq!(classified, 1, "holdership is decided once per topic");
    assert!(to_publish.is_empty());
    assert!(deferred.is_empty());
    let keys: Vec<Vec<u8>> = undeliverable.into_iter().map(|(key, _, _)| key).collect();
    assert_eq!(keys, vec![b"older".to_vec(), b"newer".to_vec()]);
}

#[tokio::test(start_paused = true)]
async fn empty_resets_backoff() {
    let _clock = freeze_clock();
    let (_dir, handler, _task_handle) = outbox_handler();
    let key = TaskKey::DrainDocumentSyncOutbox;
    handler
        .retry_backoff
        .lock()
        .expect("retry backoff mutex poisoned")
        .insert(key.clone(), 3);

    assert!(
        handler
            .open_rotation(&key, OutboxRotation::default())
            .await
            .is_none()
    );
    assert!(
        !handler
            .retry_backoff
            .lock()
            .expect("retry backoff mutex poisoned")
            .contains_key(&key)
    );
}

#[tokio::test]
async fn blocked_keeps_backoff() {
    let _clock = freeze_clock();
    let realm_id = RealmId::from_bytes([48u8; 32]);
    let temp_dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(temp_dir.path().to_str().expect("temp path")).expect("storage opens");
    let net = make_net_handle(realm_id, &storage, [48u8; 32]).await;
    tokio::time::pause();
    let task_handle = TaskHandle::new();
    let context = Arc::new(DriverContext {
        storage_handle: storage.clone(),
        net_handle: Some(net.clone()),
        blob_handle: None,
        metadata_handle: None,
        task_handle: Some(task_handle.clone()),
        compute_handle: None,
    });
    let handler =
        OperationsTaskHandler::new(context, JobsRuntime::new()).with_outbox_limits(1, 1, 1);
    let key = TaskKey::DrainDocumentSyncOutbox;
    handler
        .retry_backoff
        .lock()
        .expect("retry backoff mutex poisoned")
        .insert(key.clone(), 3);
    let mut blocked_change = change();
    blocked_change.placement = aruna_core::structs::PlacementRef {
        strategy_id: Ulid::from_bytes([48; 16]),
        shard: 1,
    };
    let record = crate::sync::document_outbox::new_outbox_record(
        node(1),
        target(),
        Vec::new(),
        DocumentSyncOutboxEvent::Upsert {
            bytes: b"blocked".to_vec(),
            change: blocked_change,
        },
        aruna_core::structs::PlacementRef::NIL,
        false,
    );
    write_outbox_record(&storage, &record).await;

    handler.drain_document_sync_outbox().await;

    assert_eq!(
        scheduled_after(&task_handle).await,
        DOCUMENT_SYNC_DEFER_RETRY_AFTER
    );
    assert_eq!(
        handler
            .retry_backoff
            .lock()
            .expect("retry backoff mutex poisoned")
            .get(&key),
        Some(&3)
    );
    shutdown_net(&net).await;
}

// A full first page of records for a genesis-less shard topic (all deferred)
// must not starve records for other topics behind it in the FIFO: the drain
// pages the whole outbox per run, so a later-page record still publishes.
#[tokio::test(start_paused = true)]
async fn deferred_head_paginates() {
    let _clock = freeze_clock();
    let realm_id = RealmId::from_bytes([44u8; 32]);
    let temp_dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(temp_dir.path().to_str().expect("temp path")).expect("storage opens");
    let net = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().expect("bind addr"),
            realm_id,
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        storage.clone(),
    )
    .await
    .expect("net handle");
    let task_handle = TaskHandle::new();
    let context = Arc::new(DriverContext {
        storage_handle: storage.clone(),
        net_handle: Some(net.clone()),
        blob_handle: None,
        metadata_handle: None,
        task_handle: Some(task_handle.clone()),
        compute_handle: None,
    });

    // Every head-page record targets one shard topic with no local genesis,
    // so all of them defer.
    let deferred_change = DocumentSyncChange {
        base: None,
        current: DocumentSyncRevision {
            generation: 1,
            event_id: Ulid::from_parts(8, 1),
            actor: node(1),
            updated_at_ms: 9,
        },
        kind: DocumentSyncChangeKind::Upsert,
        placement: aruna_core::structs::PlacementRef {
            strategy_id: Ulid::from_parts(42, 1),
            shard: 3,
        },
    };
    let deferred_target = DocumentSyncTarget::MetadataRegistry {
        group_id: Ulid::from_parts(1, 1),
        document_id: Ulid::from_parts(2, 2),
    };
    let mut writes = Vec::with_capacity(OUTBOX_DRAIN_BATCH_SIZE + 1);
    for index in 0..OUTBOX_DRAIN_BATCH_SIZE {
        let record = crate::sync::document_outbox::new_outbox_record_with_id(
            Ulid::from_parts(1, index as u128),
            node(1),
            deferred_target.clone(),
            Vec::new(),
            DocumentSyncOutboxEvent::Upsert {
                bytes: Vec::new(),
                change: deferred_change,
            },
            aruna_core::structs::PlacementRef::NIL,
            false,
        );
        writes
            .push(crate::sync::document_outbox::outbox_write_entry(&record).expect("outbox entry"));
    }

    // One later origin record for a shared (non-shard) topic, ordered
    // strictly after the head page, so only pagination reaches it.
    let publish_record = crate::sync::document_outbox::new_outbox_record_with_id(
        Ulid::from_parts(2, 0),
        node(1),
        DocumentSyncTarget::RealmAuthorization { realm_id },
        Vec::new(),
        DocumentSyncOutboxEvent::Upsert {
            bytes: b"realm-auth".to_vec(),
            change: change(),
        },
        aruna_core::structs::PlacementRef::NIL,
        true,
    );
    let publish_key = outbox_key(&publish_record).to_vec();
    writes.push(crate::sync::document_outbox::outbox_write_entry(&publish_record).expect("entry"));

    match storage
        .send_effect(Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
        other => panic!("unexpected batch write event: {other:?}"),
    }

    let handler = OperationsTaskHandler::new(context, JobsRuntime::new());
    handler.drain_document_sync_outbox().await;

    assert_eq!(
        read_outbox_record(&storage, &publish_key)
            .await
            .expect("read publish record"),
        None,
        "the later-page record must publish despite an all-deferred first page"
    );
    let remaining = read_outbox_records(&storage, &[], None, OUTBOX_DRAIN_BATCH_SIZE + 8)
        .await
        .expect("read remaining");
    assert_eq!(
        remaining.records.len(),
        OUTBOX_DRAIN_BATCH_SIZE,
        "every deferred record is retained for the next run"
    );
    assert_eq!(
        scheduled_after(&task_handle).await,
        DOCUMENT_SYNC_DEFER_RETRY_AFTER,
        "an early defer followed by a clean suffix keeps the aggregate retry"
    );

    shutdown_net(&net).await;
}

#[tokio::test]
async fn boundary_appends_wait() {
    let _clock = freeze_clock();
    let mut harness = BoundaryHarness::new().await;
    harness.seed_records().await;

    harness.handler.drain_document_sync_outbox().await;
    harness.assert_rotation(1, true, 1);
    assert_eq!(
        scheduled_after(&harness.task_handle).await,
        OUTBOX_CONTINUATION_AFTER
    );

    harness.append_records().await;
    harness.handler.drain_document_sync_outbox().await;
    harness.assert_rotation(2, true, 0);
    assert_eq!(
        scheduled_after(&harness.task_handle).await,
        DOCUMENT_SYNC_DEFER_RETRY_AFTER
    );

    harness.append_later().await;
    harness.handler.drain_document_sync_outbox().await;
    harness.assert_rotation(3, true, 1);
    assert_eq!(
        scheduled_after(&harness.task_handle).await,
        OUTBOX_CONTINUATION_AFTER
    );

    harness.handler.drain_document_sync_outbox().await;
    harness.assert_rotation(0, false, 0);
    harness.assert_appends().await;
    harness.finish_retry().await;
}

#[tokio::test]
async fn rotation_streak() {
    let _clock = freeze_clock();
    let realm_id = RealmId::from_bytes([51u8; 32]);
    let dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
    let net = make_net_handle(realm_id, &storage, [51u8; 32]).await;
    tokio::time::pause();
    let target = DocumentSyncTarget::RealmAuthorization { realm_id };
    let topic = target.sync_topic_id(realm_id, &aruna_core::structs::PlacementRef::NIL);
    net.ensure_document_sync_topics(&[topic], Vec::new())
        .expect("shared topic genesis");
    let task_handle = TaskHandle::new();
    let context = Arc::new(DriverContext {
        storage_handle: storage.clone(),
        net_handle: Some(net.clone()),
        blob_handle: None,
        metadata_handle: None,
        task_handle: Some(task_handle.clone()),
        compute_handle: None,
    });
    let handler = OperationsTaskHandler::new(context, JobsRuntime::new()).with_outbox_limits(
        1,
        1,
        OUTBOX_CONTINUATION_STREAK,
    );
    let total = u128::from(OUTBOX_CONTINUATION_STREAK) + 2;
    for index in 1..=total {
        let record = crate::sync::document_outbox::new_outbox_record_with_id(
            Ulid::from_parts(1, index),
            node(1),
            target.clone(),
            Vec::new(),
            DocumentSyncOutboxEvent::Upsert {
                bytes: index.to_be_bytes().to_vec(),
                change: change(),
            },
            aruna_core::structs::PlacementRef::NIL,
            true,
        );
        write_outbox_record(&storage, &record).await;
    }

    for expected in 1..=OUTBOX_CONTINUATION_STREAK {
        handler.drain_document_sync_outbox().await;
        assert_eq!(
            handler
                .rotation
                .lock()
                .expect("rotation lock")
                .continuations,
            expected
        );
        assert_eq!(
            scheduled_after(&task_handle).await,
            OUTBOX_CONTINUATION_AFTER
        );
    }
    handler.drain_document_sync_outbox().await;
    assert_eq!(
        handler
            .rotation
            .lock()
            .expect("rotation lock")
            .continuations,
        0
    );
    assert_eq!(
        scheduled_after(&task_handle).await,
        DOCUMENT_SYNC_DEFER_RETRY_AFTER
    );
    for _ in 0..4 {
        handler.drain_document_sync_outbox().await;
    }
    assert!(
        read_outbox_records(&storage, &[], None, 32)
            .await
            .expect("read streak records")
            .records
            .is_empty()
    );
    shutdown_net(&net).await;
}

// A realm-config change originated locally lands only in the outbox; draining
// it must kick the placement reconciler so this rank-0 node creates its shard
// topic geneses without waiting for a restart.
#[tokio::test]
async fn draining_a_local_realm_config_change_creates_rank0_shard_topics() {
    let realm_id = RealmId::from_bytes([61u8; 32]);
    let temp_dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(temp_dir.path().to_str().expect("temp path")).expect("storage opens");
    let net = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().expect("bind addr"),
            realm_id,
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        storage.clone(),
    )
    .await
    .expect("net handle");
    let task_handle = TaskHandle::new();
    let context = Arc::new(DriverContext {
        storage_handle: storage.clone(),
        net_handle: Some(net.clone()),
        blob_handle: None,
        metadata_handle: None,
        task_handle: Some(task_handle.clone()),
        compute_handle: None,
    });

    // Install the config so this sole node is rank-0 of every shard, but do
    // not run the placement reconciler yet.
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.seed_default_placement();
    config.ensure_node(net.node_id(), RealmNodeKind::Management);
    let actor = Actor {
        node_id: net.node_id(),
        user_id: UserId::nil(realm_id),
        realm_id,
    };
    match storage
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: (*realm_id.as_bytes()).into(),
            value: config.to_bytes(&actor).expect("config bytes").into(),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected realm config write: {other:?}"),
    }
    net.refresh_realm_peers_from_document(&config)
        .await
        .expect("refresh peers");

    initialize_task_incoming(context.clone(), task_handle.clone(), JobsRuntime::new()).await;

    let strategy_id = config.strategies.first().expect("a strategy").strategy_id;
    let topic = aruna_core::document::shard_topic_id(
        realm_id,
        &aruna_core::structs::PlacementRef {
            strategy_id,
            shard: 0,
        },
    );
    assert!(
        !net.document_sync_topic_exists(topic).unwrap_or(false),
        "the rank-0 shard topic must not exist before the config change is drained"
    );

    let record = crate::sync::document_outbox::new_outbox_record(
        net.node_id(),
        DocumentSyncTarget::RealmConfig { realm_id },
        Vec::new(),
        DocumentSyncOutboxEvent::Upsert {
            bytes: b"config".to_vec(),
            change: change(),
        },
        aruna_core::structs::PlacementRef::NIL,
        true,
    );
    write_outbox_record(&storage, &record).await;
    task_handle
        .send_effect(crate::sync::document_outbox::schedule_outbox_drain_effect())
        .await;

    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if net.document_sync_topic_exists(topic).unwrap_or(false) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "rank-0 shard topic was not created after the local config change drained"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    net.shutdown().await;
}

struct ConfigHarness {
    _dir: tempfile::TempDir,
    storage: aruna_storage::StorageHandle,
    net: NetHandle,
    handler: OperationsTaskHandler,
    config: RealmConfigDocument,
    realm_id: RealmId,
    placement: aruna_core::structs::PlacementRef,
    shard_target: DocumentSyncTarget,
}

async fn config_setup() -> ConfigHarness {
    let realm_id = RealmId::from_bytes([47u8; 32]);
    let temp_dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(temp_dir.path().to_str().expect("temp path")).expect("storage opens");
    let net = make_net_handle(realm_id, &storage, [47u8; 32]).await;
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.seed_default_placement();
    write_realm_config(&storage, realm_id, &config, net.node_id()).await;
    let placement = aruna_core::structs::PlacementRef {
        strategy_id: config.strategies[0].strategy_id,
        shard: 0,
    };
    let shared_target = DocumentSyncTarget::RealmAuthorization { realm_id };
    let shard_target = DocumentSyncTarget::MetadataRegistry {
        group_id: Ulid::from_parts(7, 1),
        document_id: Ulid::from_parts(8, 1),
    };
    let shared_topic =
        shared_target.sync_topic_id(realm_id, &aruna_core::structs::PlacementRef::NIL);
    net.ensure_document_sync_topics(&[shared_topic], Vec::new())
        .expect("shared topic genesis");
    let mut shard_change = change();
    shard_change.placement = placement;
    let shared = crate::sync::document_outbox::new_outbox_record_with_id(
        Ulid::from_parts(1, 1),
        node(1),
        shared_target,
        Vec::new(),
        DocumentSyncOutboxEvent::Upsert {
            bytes: b"shared".to_vec(),
            change: change(),
        },
        aruna_core::structs::PlacementRef::NIL,
        true,
    );
    let shard = crate::sync::document_outbox::new_outbox_record_with_id(
        Ulid::from_parts(1, 2),
        node(1),
        shard_target.clone(),
        Vec::new(),
        DocumentSyncOutboxEvent::Upsert {
            bytes: b"shard".to_vec(),
            change: shard_change,
        },
        placement,
        true,
    );
    write_outbox_record(&storage, &shared).await;
    write_outbox_record(&storage, &shard).await;

    let context = Arc::new(DriverContext {
        storage_handle: storage.clone(),
        net_handle: Some(net.clone()),
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let handler =
        OperationsTaskHandler::new(context, JobsRuntime::new()).with_outbox_limits(1, 1, 2);
    ConfigHarness {
        _dir: temp_dir,
        storage,
        net,
        handler,
        config,
        realm_id,
        placement,
        shard_target,
    }
}

// Post-rebalance genesis adoption: live holders no longer include the emit-time
// stamped holder carrying the genesis, so the bootstrap pull must union stamp and
// live holders; otherwise a fresh genesis could fork the topic and evict writes.
#[tokio::test]
async fn pull_reaches_ex_holder() {
    let realm_id = RealmId::from_bytes([53u8; 32]);
    let ex_dir = tempdir().expect("temp dir");
    let ex_storage =
        FjallStorage::open(ex_dir.path().to_str().expect("temp path")).expect("storage opens");
    let ex_holder = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().expect("bind addr"),
            realm_id,
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        ex_storage.clone(),
    )
    .await
    .expect("net handle");
    let temp_dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(temp_dir.path().to_str().expect("temp path")).expect("storage opens");
    let net = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().expect("bind addr"),
            realm_id,
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        storage.clone(),
    )
    .await
    .expect("net handle");
    net.add_peer_addr(ex_holder.endpoint_addr()).await;
    ex_holder.add_peer_addr(net.endpoint_addr()).await;
    // The ex-holder must serve inbound sync streams for the pull to reach
    // its genesis.
    crate::sync::incoming::initialize_net_incoming(Arc::new(DriverContext {
        storage_handle: ex_storage.clone(),
        net_handle: Some(ex_holder.clone()),
        blob_handle: None,
        metadata_handle: None,
        task_handle: Some(TaskHandle::new()),
        compute_handle: None,
    }));

    // The live config resolves the shard's holders to this node only: the
    // stamped ex-holder has been rebalanced out.
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.seed_default_placement();
    config.ensure_node(net.node_id(), RealmNodeKind::Management);
    let actor = Actor {
        node_id: net.node_id(),
        user_id: UserId::nil(realm_id),
        realm_id,
    };
    match storage
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: (*realm_id.as_bytes()).into(),
            value: config.to_bytes(&actor).expect("config bytes").into(),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected realm config write: {other:?}"),
    }
    // The ex-holder keeps the realm config that rebalanced it out, so it
    // still admits inbound sync from the current holders.
    ex_holder
        .refresh_realm_peers_from_document(&config)
        .await
        .expect("ex-holder refreshes realm peers");

    let strategy_id = config.strategies.first().expect("a strategy").strategy_id;
    let placement = aruna_core::structs::PlacementRef {
        strategy_id,
        shard: 0,
    };
    let target = DocumentSyncTarget::MetadataRegistry {
        group_id: Ulid::from_parts(1, 1),
        document_id: Ulid::from_parts(2, 2),
    };
    let topic = target.sync_topic_id(realm_id, &placement);

    // Only the ex-holder carries the genesis (with this node as a member,
    // as the pre-rebalance membership reconciliation would have left it).
    ex_holder
        .ensure_document_sync_topics(&[topic], vec![net.node_id()])
        .expect("genesis on the ex-holder");
    assert!(!net.document_sync_topic_exists(topic).unwrap_or(true));

    let mut change = change();
    change.placement = placement;
    let record = crate::sync::document_outbox::new_outbox_record(
        net.node_id(),
        target,
        vec![ex_holder.node_id()],
        DocumentSyncOutboxEvent::Upsert {
            bytes: b"doc".to_vec(),
            change,
        },
        aruna_core::structs::PlacementRef::NIL,
        false,
    );
    let record_key = outbox_key(&record).to_vec();
    write_outbox_record(&storage, &record).await;

    let context = Arc::new(DriverContext {
        storage_handle: storage.clone(),
        net_handle: Some(net.clone()),
        blob_handle: None,
        metadata_handle: None,
        task_handle: Some(TaskHandle::new()),
        compute_handle: None,
    });
    let handler = OperationsTaskHandler::new(context, JobsRuntime::new());
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        handler.drain_document_sync_outbox().await;
        if read_outbox_record(&storage, &record_key)
            .await
            .expect("read outbox record")
            .is_none()
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "record never published: the drain did not adopt the ex-holder's genesis"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        net.document_sync_topic_exists(topic).unwrap_or(false),
        "the genesis must be adopted from the stamped ex-holder"
    );

    ex_holder.shutdown().await;
    net.shutdown().await;
}

fn admin_placement(shard: u32) -> aruna_core::structs::PlacementRef {
    aruna_core::structs::PlacementRef {
        strategy_id: Ulid::from_bytes([50; 16]),
        shard,
    }
}

fn admin_outbox(
    realm_id: RealmId,
    origin: aruna_core::NodeId,
    origin_seq: u64,
    target: DocumentSyncTarget,
    placement: aruna_core::structs::PlacementRef,
) -> DocumentSyncOutboxRecord {
    use aruna_core::admin_documents::{
        AdminDocumentClock, AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget,
    };
    let user_id = aruna_core::types::UserId::nil(realm_id);
    crate::sync::document_outbox::new_outbox_record(
        node(1),
        target,
        Vec::new(),
        DocumentSyncOutboxEvent::admin(AdminDocumentEvent {
            event_id: ulid::Ulid::from_parts(9, u128::from(origin_seq)),
            target: AdminDocumentTarget::User { user_id },
            origin_node_id: origin,
            origin_seq,
            observed: AdminDocumentClock::default(),
            actor: aruna_core::structs::Actor {
                node_id: node(1),
                user_id,
                realm_id,
            },
            op: AdminDocumentOperation::UserNameSet {
                name: format!("user-{origin_seq}"),
            },
        }),
        placement,
        false,
    )
}

fn admin_record(origin: aruna_core::NodeId, origin_seq: u64) -> DocumentSyncOutboxRecord {
    admin_outbox(
        RealmId([3; 32]),
        origin,
        origin_seq,
        target(),
        admin_placement(1),
    )
}

fn shard_change(seed: u8) -> DocumentSyncChange {
    let mut value = change();
    value.placement = aruna_core::structs::PlacementRef {
        strategy_id: Ulid::from_bytes([seed; 16]),
        shard: 1,
    };
    value
}

fn shard_topic_record(origin_seq: u64) -> DocumentSyncOutboxRecord {
    crate::sync::document_outbox::new_outbox_record(
        node(1),
        target(),
        vec![node(2)],
        DocumentSyncOutboxEvent::Upsert {
            bytes: vec![origin_seq as u8],
            change: change(),
        },
        aruna_core::structs::PlacementRef::NIL,
        false,
    )
}

struct BoundaryHarness {
    _dir: tempfile::TempDir,
    storage: aruna_storage::StorageHandle,
    net: NetHandle,
    task_handle: TaskHandle,
    handler: OperationsTaskHandler,
    realm_id: RealmId,
    appended: Vec<(Vec<u8>, DocumentSyncOutboxRecord)>,
}

impl BoundaryHarness {
    async fn new() -> Self {
        let realm_id = RealmId::from_bytes([45u8; 32]);
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        let net = make_net_handle(realm_id, &storage, [45u8; 32]).await;
        tokio::time::pause();
        let task_handle = TaskHandle::new();
        let context = Arc::new(DriverContext {
            storage_handle: storage.clone(),
            net_handle: Some(net.clone()),
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(task_handle.clone()),
            compute_handle: None,
        });
        let handler =
            OperationsTaskHandler::new(context, JobsRuntime::new()).with_outbox_limits(1, 1, 1);
        Self {
            _dir: dir,
            storage,
            net,
            task_handle,
            handler,
            realm_id,
            appended: Vec::new(),
        }
    }

    fn placed_change(&self) -> DocumentSyncChange {
        let mut value = change();
        value.placement = aruna_core::structs::PlacementRef {
            strategy_id: Ulid::from_bytes([45; 16]),
            shard: 1,
        };
        value
    }

    fn record(
        &self,
        id: u128,
        target: DocumentSyncTarget,
        event: DocumentSyncOutboxEvent,
    ) -> DocumentSyncOutboxRecord {
        crate::sync::document_outbox::new_outbox_record_with_id(
            Ulid::from_parts(1, id),
            node(1),
            target,
            Vec::new(),
            event,
            aruna_core::structs::PlacementRef::NIL,
            true,
        )
    }

    async fn seed_records(&self) {
        for id in 1..=3 {
            let record = self.record(
                id,
                target(),
                DocumentSyncOutboxEvent::Upsert {
                    bytes: id.to_be_bytes().to_vec(),
                    change: self.placed_change(),
                },
            );
            write_outbox_record(&self.storage, &record).await;
        }
        let initial = self.record(
            0,
            DocumentSyncTarget::RealmAuthorization {
                realm_id: self.realm_id,
            },
            DocumentSyncOutboxEvent::Delete { change: change() },
        );
        write_outbox_record(&self.storage, &initial).await;
    }

    async fn append_records(&mut self) {
        let shared = DocumentSyncTarget::RealmAuthorization {
            realm_id: self.realm_id,
        };
        let topic = shared.sync_topic_id(self.realm_id, &aruna_core::structs::PlacementRef::NIL);
        self.net
            .ensure_document_sync_topics(&[topic], Vec::new())
            .expect("appended topic genesis");
        let records = [
            self.record(
                4,
                shared,
                DocumentSyncOutboxEvent::Delete { change: change() },
            ),
            self.record(
                5,
                target(),
                DocumentSyncOutboxEvent::Upsert {
                    bytes: b"appended-upsert".to_vec(),
                    change: self.placed_change(),
                },
            ),
        ];
        for record in records {
            let key = outbox_key(&record).to_vec();
            write_outbox_record(&self.storage, &record).await;
            self.appended.push((key, record));
        }
    }

    async fn append_later(&mut self) {
        let record = self.record(
            6,
            target(),
            DocumentSyncOutboxEvent::Upsert {
                bytes: b"appended-later".to_vec(),
                change: self.placed_change(),
            },
        );
        let key = outbox_key(&record).to_vec();
        write_outbox_record(&self.storage, &record).await;
        self.appended.push((key, record));
    }

    fn assert_rotation(&self, examined: usize, cursor: bool, continuations: u32) {
        let rotation = self.handler.rotation.lock().expect("rotation lock");
        assert_eq!(
            (rotation.totals.examined, rotation.cursor.is_some()),
            (examined, cursor)
        );
        assert_eq!(rotation.continuations, continuations);
    }

    async fn assert_appends(&self) {
        for (key, record) in &self.appended {
            assert_eq!(
                read_outbox_record(&self.storage, key)
                    .await
                    .expect("read appended record"),
                Some(record.clone())
            );
        }
    }

    async fn finish_retry(self) {
        let shard_topic = target().sync_topic_id(self.realm_id, &self.placed_change().placement);
        self.net
            .ensure_document_sync_topics(&[shard_topic], Vec::new())
            .expect("blocked head topic genesis");
        for _ in 0..6 {
            self.handler.drain_document_sync_outbox().await;
        }
        let remaining = read_outbox_records(&self.storage, &[], None, 8)
            .await
            .expect("read retried records");
        assert!(
            remaining.records.is_empty(),
            "records across all streams must retry after the rotation closes"
        );
        shutdown_net(&self.net).await;
    }
}
