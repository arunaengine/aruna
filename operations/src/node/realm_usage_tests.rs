//! State and event tests of the realm usage sum and the rebuild marker read.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::NodeId;
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{NODE_STATS_KEYSPACE, REALM_CONFIG_KEYSPACE, USAGE_STATS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::operation_check::{
    check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::structs::storage::usage::{
    NodeUsageSnapshot, SUMMARY_GLOBAL_KEY, USAGE_GLOBAL_PREFIX, USAGE_REBUILD_KEY, UsageCounters,
    global_shard_keys, usage_global_key, usage_group_key, usage_group_prefix, usage_summary_key,
};
use aruna_core::types::{GroupId, Key, Value};
use ulid::Ulid;

use super::{RealmUsageError, RealmUsageOperation, UsageCompleteOperation};
use crate::node::usage_stats::{RealmUsage, RealmUsageScope};

const REALM: RealmId = RealmId([7; 32]);
const GROUP: GroupId = Ulid::from_parts(4, 4);

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn counters(objects: u64, logical_bytes: u64) -> UsageCounters {
    UsageCounters {
        objects,
        logical_bytes,
        ..UsageCounters::default()
    }
}

/// A realm whose sync-eligible nodes are 1 and 2; node 3 is a user device.
fn config_bytes() -> Value {
    let mut config = RealmConfigDocument::default_for_realm(REALM, Vec::new());
    config.seed_default_placement();
    config.ensure_node(node(1), RealmNodeKind::Server);
    config.ensure_node(node(2), RealmNodeKind::Server);
    let owner = aruna_core::UserId::new(Ulid::from_bytes([9; 16]), REALM);
    config.ensure_node(node(3), RealmNodeKind::User { owner });
    Value::from(config.to_bytes().unwrap())
}

fn snapshot_row(key: Vec<u8>, node_id: NodeId, used: UsageCounters, replicas: u64) -> (Key, Value) {
    let snapshot = NodeUsageSnapshot {
        node_id,
        counters: used,
        replicas: counters(replicas, replicas),
        revisions: BTreeMap::new(),
    };
    (Key::from(key), Value::from(snapshot.to_bytes().unwrap()))
}

fn storage(event: StorageEvent) -> Event {
    Event::Storage(event)
}

fn read(value: Option<Value>) -> Event {
    storage(StorageEvent::ReadResult {
        key: Key::from(Vec::new()),
        value,
    })
}

fn page(values: Vec<(Key, Value)>, next: Option<Key>) -> Event {
    storage(StorageEvent::IterResult {
        values,
        next_start_after: next,
    })
}

fn snapshots(prefix: Vec<u8>, start: Option<Key>) -> Effect {
    Effect::Storage(StorageEffect::Iter {
        key_space: NODE_STATS_KEYSPACE.to_string(),
        prefix: Some(Key::from(prefix)),
        start: start.map(IterStart::After),
        limit: 1_000,
        txn_id: None,
    })
}

fn config_read() -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: REALM_CONFIG_KEYSPACE.to_string(),
        key: DocumentTarget::RealmConfig { realm_id: REALM }.storage_key(),
        txn_id: None,
    })
}

fn global_read() -> Effect {
    Effect::Storage(StorageEffect::BatchRead {
        reads: global_shard_keys()
            .into_iter()
            .map(|key| (USAGE_STATS_KEYSPACE.to_string(), Key::from(key)))
            .collect(),
        txn_id: None,
    })
}

fn group_read() -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: USAGE_STATS_KEYSPACE.to_string(),
        key: Key::from(usage_group_key(GROUP)),
        txn_id: None,
    })
}

fn shard_values() -> Event {
    let shards = global_shard_keys();
    let value = |used: UsageCounters| Some(Value::from(used.to_bytes().unwrap()));
    storage(StorageEvent::BatchReadResult {
        values: vec![
            (Key::from(shards[0].clone()), value(counters(1, 10))),
            (Key::from(shards[1].clone()), None),
            (Key::from(shards[2].clone()), value(counters(2, 5))),
        ],
    })
}

/// Node 1 is local, node 2 counts, node 3 is a device and a row of node 4 names node 2.
fn global_page() -> Vec<(Key, Value)> {
    vec![
        snapshot_row(usage_global_key(node(1)), node(1), counters(50, 50), 1),
        snapshot_row(usage_global_key(node(2)), node(2), counters(4, 40), 2),
        snapshot_row(usage_global_key(node(3)), node(3), counters(9, 9), 9),
        snapshot_row(usage_global_key(node(4)), node(2), counters(9, 9), 9),
    ]
}

fn summary_write(global: &RealmUsage, group: &RealmUsage) -> Effect {
    let row = |key: Vec<u8>, total: &RealmUsage| {
        let value = total.counters.to_bytes().unwrap();
        (NODE_STATS_KEYSPACE.to_string(), Key::from(key), Value::from(value))
    };
    Effect::Storage(StorageEffect::BatchWrite {
        writes: vec![
            row(SUMMARY_GLOBAL_KEY.to_vec(), global),
            row(usage_summary_key(GROUP), group),
        ],
        txn_id: None,
    })
}

#[test]
fn summary_sums_scopes() {
    let mut operation = RealmUsageOperation::summary(node(1), Some(REALM), true, vec![GROUP]);
    let continued = Key::from(usage_global_key(node(2)));
    let global = RealmUsage::of(counters(3, 15), (counters(4, 40), counters(3, 3))).unwrap();
    let group = RealmUsage::of(counters(0, 0), (counters(0, 0), counters(0, 0))).unwrap();
    check_trace(
        &mut operation,
        &[config_read()],
        vec![
            (read(Some(config_bytes())), vec![global_read()]),
            (
                shard_values(),
                vec![snapshots(USAGE_GLOBAL_PREFIX.to_vec(), None)],
            ),
            (
                page(global_page(), Some(continued.clone())),
                vec![snapshots(USAGE_GLOBAL_PREFIX.to_vec(), Some(continued))],
            ),
            (page(Vec::new(), None), vec![group_read()]),
            (read(None), vec![snapshots(usage_group_prefix(GROUP), None)]),
            (page(Vec::new(), None), vec![summary_write(&global, &group)]),
            (
                storage(StorageEvent::BatchWriteResult {
                    entries: Vec::new(),
                }),
                vec![],
            ),
        ],
    );
    assert!(operation.is_complete());
    assert_eq!(operation.finalize().unwrap(), vec![global, group]);
}

#[test]
fn unknown_realm_counts_all() {
    // Without a realm config every node's snapshot counts, and a sum writes nothing.
    let mut operation = RealmUsageOperation::new(node(1), None, RealmUsageScope::Global);
    check_trace(
        &mut operation,
        &[global_read()],
        vec![
            (
                storage(StorageEvent::BatchReadResult { values: Vec::new() }),
                vec![snapshots(USAGE_GLOBAL_PREFIX.to_vec(), None)],
            ),
            (page(global_page(), None), vec![]),
        ],
    );
    let total = RealmUsage::of(counters(0, 0), (counters(13, 49), counters(12, 12))).unwrap();
    assert_eq!(operation.finalize().unwrap(), vec![total]);
}

#[test]
fn storage_errors_end() {
    let error = || {
        storage(StorageEvent::Error {
            error: StorageError::Timeout,
        })
    };
    let prefixes: Vec<Vec<Event>> = vec![
        vec![],
        vec![read(Some(config_bytes()))],
        vec![read(Some(config_bytes())), shard_values()],
        vec![
            read(Some(config_bytes())),
            shard_values(),
            page(Vec::new(), None),
            read(None),
            page(Vec::new(), None),
        ],
    ];
    for events in prefixes {
        let mut operation = RealmUsageOperation::summary(node(1), Some(REALM), true, vec![GROUP]);
        operation.start();
        for event in events {
            assert!(!operation.step(event).is_empty());
        }
        assert!(operation.step(error()).is_empty());
        assert!(operation.is_complete());
        let result = operation.finalize();
        assert_eq!(result, Err(RealmUsageError::Storage(StorageError::Timeout)));
    }
}

#[test]
fn rejects_stray_events() {
    let started = || {
        let mut operation = RealmUsageOperation::new(node(1), Some(REALM), RealmUsageScope::Global);
        operation.start();
        operation
    };
    let expected = |event: &Event| {
        matches!(
            event,
            Event::Storage(StorageEvent::ReadResult { .. } | StorageEvent::Error { .. })
        )
    };
    let refusals = rejects_events(started, sample_events, expected);
    assert!(!refusals.is_empty());
    for (label, effects, error) in refusals {
        assert!(effects.is_empty(), "{label} emitted effects");
        assert!(
            matches!(error, RealmUsageError::UnexpectedEvent(_)),
            "{label}: {error:?}"
        );
    }
    // A group scope answers with one row, never a batch.
    let mut group = RealmUsageOperation::new(node(1), None, RealmUsageScope::Group(GROUP));
    group.start();
    assert!(group.step(shard_values()).is_empty());
    assert!(matches!(
        group.finalize(),
        Err(RealmUsageError::UnexpectedEvent(_))
    ));
}

#[test]
fn abort_needs_nothing() {
    let mut operation = RealmUsageOperation::summary(node(1), Some(REALM), true, vec![GROUP]);
    check_abort(&mut operation, &[]);
    operation.start();
    check_abort(&mut operation, &[]);
    operation.step(read(None));
    check_abort(&mut operation, &[]);
    rejects_finalize(operation);
    rejects_finalize(RealmUsageOperation::new(
        node(1),
        None,
        RealmUsageScope::Global,
    ));
}

#[test]
fn marker_marks_incomplete() {
    let marker_read = Effect::Storage(StorageEffect::Read {
        key_space: USAGE_STATS_KEYSPACE.to_string(),
        key: Key::from(USAGE_REBUILD_KEY.to_vec()),
        txn_id: None,
    });
    for (value, complete) in [(None, true), (Some(Value::from(vec![1])), false)] {
        let mut operation = UsageCompleteOperation::new();
        check_trace(&mut operation, &[marker_read.clone()], vec![(read(value), vec![])]);
        assert_eq!(operation.finalize(), Ok(complete));
    }
    let mut failed = UsageCompleteOperation::new();
    failed.start();
    failed.step(storage(StorageEvent::Error {
        error: StorageError::Timeout,
    }));
    assert_eq!(failed.finalize(), Err(StorageError::Timeout));
    let refusals = rejects_events(UsageCompleteOperation::new, sample_events, |_| false);
    assert_eq!(refusals.len(), sample_events().len());
    let mut started = UsageCompleteOperation::new();
    check_abort(&mut started, &[]);
    started.start();
    check_abort(&mut started, &[]);
    rejects_finalize(started);
}
