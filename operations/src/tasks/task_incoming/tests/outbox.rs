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
