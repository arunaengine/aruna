//! Tests how drained outbox records split into publish-now, deferred and undeliverable records
//! while each admin origin keeps its order.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::admin_documents::{
    AdminDocumentClock, AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget,
};
use aruna_core::document::{
    DocumentChange, DocumentChangeKind, DocumentSyncRevision, DocumentTarget,
};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::user::id::UserId;
use ulid::Ulid;

use crate::sync::outbox::record::new_identified_record;

fn node(seed: u8) -> NodeId {
    let mut bytes = [0u8; 32];
    bytes[0] = seed;
    iroh::SecretKey::from_bytes(&bytes).public()
}

fn target() -> DocumentTarget {
    DocumentTarget::Group {
        group_id: Ulid::from_parts(7, 1),
    }
}

fn admin_record(origin: NodeId, origin_seq: u64) -> DocumentOutboxRecord {
    let realm_id = RealmId([3; 32]);
    let user_id = UserId::nil(realm_id);
    let event = AdminDocumentEvent {
        event_id: Ulid::from_parts(9, u128::from(origin_seq)),
        target: AdminDocumentTarget::User { user_id },
        origin_node_id: origin,
        origin_seq,
        observed: AdminDocumentClock::default(),
        actor: Actor {
            node_id: node(1),
            user_id,
            realm_id,
        },
        op: AdminDocumentOperation::UserNameSet {
            name: format!("user-{origin_seq}"),
        },
    };
    let placement = PlacementRef {
        strategy_id: Ulid::from_bytes([50; 16]),
        shard: 1,
    };
    let id = Ulid::from_parts(10, u128::from(origin_seq));
    let event = DocumentOutboxEvent::admin(event);
    new_identified_record(id, node(1), target(), Vec::new(), event, placement, false)
}

fn shard_topic_record(origin_seq: u64) -> DocumentOutboxRecord {
    let current = DocumentSyncRevision {
        generation: 1,
        event_id: Ulid::from_parts(8, 1),
        actor: node(1),
        updated_at_ms: 9,
    };
    let change = DocumentChange {
        base: None,
        current,
        kind: DocumentChangeKind::Upsert,
        placement: PlacementRef::NIL,
    };
    let event = DocumentOutboxEvent::Upsert {
        bytes: vec![origin_seq as u8],
        change,
    };
    let id = Ulid::from_parts(11, u128::from(origin_seq));
    let (peers, placement) = (vec![node(2)], PlacementRef::NIL);
    new_identified_record(id, node(1), target(), peers, event, placement, false)
}

#[test]
fn admin_block_spans() {
    let origin = node(4);
    let blocked_topic = TopicId::hash(b"blocked-admin-page");
    let healthy_topic = TopicId::hash(b"healthy-admin-page");
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

// A blocked admin operation blocks its later origin sequence for the rotation across all topics.
// Publishing a later sequence first would make the earlier record stale.
#[test]
fn admin_origin_blocks() {
    let origin = node(4);
    let blocked_topic = TopicId::hash(b"blocked-admin-topic");
    let healthy_topic = TopicId::hash(b"healthy-admin-topic");
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

// A topic a failure blocked holds a later shared-topic record too, and a held admin record
// blocks its origin for the records after it.
#[test]
fn shared_topic_blocked() {
    let topic = TopicId::hash(b"shared-topic");
    let mut defer = DrainDeferState::default();
    defer.deferred_topics.insert(topic);
    let mut shared = shard_topic_record(1);
    shared.target = DocumentTarget::RealmConfig {
        realm_id: RealmId([3; 32]),
    };
    let origin = node(4);
    let records = vec![
        (b"shared".to_vec(), shared, topic),
        (b"admin".to_vec(), admin_record(origin, 1), topic),
    ];
    let (to_publish, deferred, undeliverable) =
        partition_drain_records(records, &mut defer, true, |_| true, |_| DeferOutcome::Retry);
    assert!(to_publish.is_empty() && undeliverable.is_empty());
    assert_eq!(deferred.len(), 2);
    assert!(defer.blocked_origins.contains(&origin));
}

// A record held by its blocked origin holds its topic, so a later record of another writer on
// that topic stays behind it.
#[test]
fn held_origin_topic() {
    let (origin, topic) = (node(4), TopicId::hash(b"held-topic"));
    let mut defer = DrainDeferState::default();
    defer.blocked_origins.insert(origin);
    let records = vec![
        (b"admin".to_vec(), admin_record(origin, 1), topic),
        (b"later".to_vec(), shard_topic_record(2), topic),
    ];
    let (to_publish, deferred, _) =
        partition_drain_records(records, &mut defer, true, |_| true, |_| DeferOutcome::Retry);
    assert!(to_publish.is_empty());
    assert_eq!(deferred.len(), 2);
}

// A relay waits while an earlier record of its origin still publishes on this page; a relay of
// another origin to another shard runs.
#[test]
fn later_relay_held() {
    let (origin, other) = (node(4), node(5));
    let (topic, elsewhere) = (TopicId::hash(b"relay-topic"), TopicId::hash(b"other-topic"));
    let to_publish = [(b"admin/1".to_vec(), admin_record(origin, 1), topic)];
    let undeliverable = vec![
        (b"admin/2".to_vec(), admin_record(origin, 2), topic),
        (b"admin/3".to_vec(), admin_record(other, 1), elsewhere),
    ];
    let mut defer = DrainDeferState::default();
    let (now, held) = hold_relays(&to_publish, undeliverable, &mut defer);
    let keys = |records: &[DrainRecord]| records.iter().map(|(key, ..)| key.clone()).collect();
    let (now, held): (Vec<Vec<u8>>, Vec<Vec<u8>>) = (keys(&now), keys(&held));
    assert_eq!(
        (now, held),
        (vec![b"admin/3".to_vec()], vec![b"admin/2".to_vec()])
    );
}

// An origin waiting for a sync on one topic publishes there; on another topic its record waits
// and holds the origin, so no later record of it overtakes the held one.
#[test]
fn sync_wait_topics() {
    let origin = node(4);
    let (waiting, other) = (TopicId::hash(b"waiting"), TopicId::hash(b"other"));
    let mut defer = DrainDeferState::default();
    add_waits(&mut defer.sync_waits, &[(origin, waiting, [1; 32])]);
    let records = vec![
        (b"first".to_vec(), admin_record(origin, 1), waiting),
        (b"second".to_vec(), admin_record(origin, 2), other),
        (b"third".to_vec(), admin_record(origin, 3), waiting),
    ];
    let (to_publish, deferred, _) =
        partition_drain_records(records, &mut defer, true, |_| true, |_| DeferOutcome::Retry);
    let keys = |records: &[DrainRecord]| -> Vec<Vec<u8>> {
        records.iter().map(|(key, ..)| key.clone()).collect()
    };
    assert_eq!(keys(&to_publish), vec![b"first".to_vec()]);
    assert_eq!(keys(&deferred), vec![b"second".to_vec(), b"third".to_vec()]);
}

// A held relay holds its shard topic, here and on the next pages, so a later relay of another
// origin to that shard waits behind it.
#[test]
fn relay_topic_held() {
    let (origin, other) = (node(4), node(5));
    let (published, shard) = (TopicId::hash(b"published"), TopicId::hash(b"shard"));
    let to_publish = [(b"admin/1".to_vec(), admin_record(origin, 1), published)];
    let undeliverable = vec![
        (b"admin/2".to_vec(), admin_record(origin, 2), shard),
        (b"admin/3".to_vec(), admin_record(other, 1), shard),
    ];
    let mut defer = DrainDeferState::default();
    let (now, held) = hold_relays(&to_publish, undeliverable, &mut defer);
    assert!(now.is_empty());
    assert_eq!(held.len(), 2);
    assert!(defer.deferred_topics.contains(&shard));
}

// An origin waiting on two topics publishes on neither: the sync of one cannot carry the
// earlier record on the other.
#[test]
fn split_waits_hold() {
    let origin = node(4);
    let (first, second) = (TopicId::hash(b"first-wait"), TopicId::hash(b"second-wait"));
    let mut defer = DrainDeferState::default();
    add_waits(&mut defer.sync_waits, &[(origin, first, [1; 32]), (origin, second, [1; 32])]);
    let records = vec![(b"later".to_vec(), admin_record(origin, 3), first)];
    let (to_publish, deferred, _) =
        partition_drain_records(records, &mut defer, true, |_| true, |_| DeferOutcome::Retry);
    assert!(to_publish.is_empty());
    assert_eq!(deferred.len(), 1);
}

#[test]
fn topic_block_spans() {
    let topic = TopicId::hash(b"blocked-topic-pages");
    let healthy_topic = TopicId::hash(b"healthy-topic-pages");
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

// Evaluate topic availability once so adjacent records cannot split across defer and publish.
// Publishing the newer record first would invert the origin sequence.
#[test]
fn drain_partition_flips() {
    let topic = TopicId::hash(b"shard-genesis-race");
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
fn drain_partition_order() {
    let topic = TopicId::hash(b"shard-genesis-present");
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

// A non-holder can neither create nor join a bucket topic, so it cannot publish the record.
// It is classified for reporting and retained unless a relay succeeds.
#[test]
fn unheld_bucket_undeliverable() {
    let topic = TopicId::hash(b"unheld-bucket");
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
