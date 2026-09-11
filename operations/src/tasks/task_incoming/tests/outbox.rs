use super::super::*;
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
    let blocked = crate::sync::document_sync_outbox::new_outbox_record_with_id(
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
    let healthy = crate::sync::document_sync_outbox::new_outbox_record_with_id(
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
