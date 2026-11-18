//! Tests the routing input operations against a fixed set of stored rows, step by step.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, HashMap};
use std::time::SystemTime;

use aruna_core::UserId;
use aruna_core::calls::ChildOutcome;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{S3_BUCKET_KEYSPACE, STORAGE_ROUTING_KEYSPACE};
use aruna_core::operation::{EffectOperation, Runnable};
use aruna_core::structs::storage::blob::{BackendRef, BucketInfo, ResolvedBackend};
use aruna_core::structs::storage::cleanup::CleanupStrategy;
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::group_backend::{GroupBackendKind, GroupStorage};
use aruna_core::structs::storage::routing::{
    BackendCatalog, GroupStorageRouting, NodeRouting, RoutingTarget, StorageRoutingRule,
    resolve_backend,
};
use aruna_core::structs::storage::usage::{UsageCounters, usage_backend_key, usage_backend_keys};
use aruna_core::types::{GroupId, Key};
use ulid::Ulid;

use super::*;
use crate::groups::backends::record_writes;

type Rows = BTreeMap<(String, Vec<u8>), Vec<u8>>;

/// Answers one storage read effect from `rows`, the way the storage backend would; a child
/// operation runs to its end against the same rows.
fn answer(effect: Effect, rows: &Rows) -> Event {
    let effect = match effect {
        Effect::Storage(effect) => effect,
        Effect::SubOperation(mut child) => {
            let mut effects = child.start().into_vec();
            while !child.is_complete() {
                let effect = effects.pop().expect("a running child waits on one effect");
                effects = child.step(answer(effect, rows)).into_vec();
            }
            return child.finalize();
        }
        other => panic!("only storage effects are expected, got {other:?}"),
    };
    let row = |space: &str, key: &[u8]| rows.get(&(space.to_string(), key.to_vec())).cloned();
    match &effect {
        StorageEffect::Read { key_space, key, .. } => Event::Storage(StorageEvent::ReadResult {
            key: key.clone(),
            value: row(key_space, key).map(Into::into),
        }),
        StorageEffect::BatchRead { reads, .. } => Event::Storage(StorageEvent::BatchReadResult {
            values: (reads.iter())
                .map(|(space, key)| (key.clone(), row(space, key).map(Into::into)))
                .collect(),
        }),
        StorageEffect::Iter {
            key_space,
            prefix,
            start,
            ..
        } => {
            assert!(start.is_none() || matches!(start, Some(IterStart::After(_))));
            let values = (rows.iter())
                .filter(|((space, key), _)| {
                    space == key_space
                        && prefix.as_ref().is_none_or(|prefix| key.starts_with(prefix))
                })
                .map(|((_, key), value)| (key.clone().into(), value.clone().into()))
                .collect();
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after: None,
            })
        }
        other => panic!("unexpected storage effect {other:?}"),
    }
}

/// Runs `operation` to its end against `rows`.
fn run<K, O: Runnable<K>>(operation: O, rows: &Rows) -> Result<O::Output, O::Error> {
    let mut operation = operation.into_machine();
    let mut effects = operation.start().into_vec();
    while !operation.is_complete() {
        let effect = effects
            .pop()
            .expect("a running operation waits on one effect");
        assert!(effects.is_empty(), "one read at a time");
        effects = operation.step(answer(effect, rows)).into_vec();
    }
    operation.finalize()
}

fn group_backend(group_id: GroupId) -> GroupStorage {
    GroupStorage {
        backend_id: Ulid::from_parts(9, 9),
        group_id,
        name: "tenant".to_string(),
        kind: GroupBackendKind::S3,
        public_config: HashMap::new(),
        created_at: SystemTime::UNIX_EPOCH,
        created_by: UserId::default(),
        disabled: false,
        cleanup: CleanupStrategy::Retain,
        updated_at: SystemTime::UNIX_EPOCH,
    }
}

fn with_backend(rows: &mut Rows, record: &GroupStorage) {
    for (space, key, value) in record_writes(record).unwrap() {
        rows.insert((space, key.to_vec()), value.to_vec());
    }
}

fn with_default(rows: &mut Rows, group_id: GroupId, target: RoutingTarget) {
    let record = GroupStorageRouting {
        group_id,
        default_target: Some(target),
        updated_at: SystemTime::UNIX_EPOCH,
        updated_by: UserId::default(),
    };
    let key = (
        STORAGE_ROUTING_KEYSPACE.to_string(),
        group_id.to_bytes().to_vec(),
    );
    rows.insert(key, record.to_bytes().unwrap());
}

fn bucket(group_id: GroupId, rules: Vec<StorageRoutingRule>) -> BucketInfo {
    BucketInfo {
        group_id,
        created_at: SystemTime::UNIX_EPOCH,
        created_by: UserId::default(),
        cors_configuration: None,
        storage_routing: rules,
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Zstd { level: 5 },
        distribution: None,
    }
}

fn capped(quota: u64) -> NodeRouting {
    NodeRouting {
        rules: Vec::new(),
        catalog: BackendCatalog::new("cold")
            .with_backend("cold", None)
            .with_quota("cold", Some(quota)),
    }
}

fn with_usage(rows: &mut Rows, backend: &str, shard: usize, bytes: u64) {
    let counters = UsageCounters {
        stored_bytes: bytes,
        ..Default::default()
    };
    let key = usage_backend_key(&BackendRef::Node(backend.to_string()), shard as _);
    rows.insert(
        (USAGE_STATS_KEYSPACE.to_string(), key),
        counters.to_bytes().unwrap(),
    );
}

#[test]
fn routes_group_backend() {
    // A group default naming a registered backend resolves, and only this group's backends load.
    let group_id = Ulid::from_parts(1, 1);
    let record = group_backend(group_id);
    let mut rows = Rows::new();
    with_backend(&mut rows, &record);
    with_default(
        &mut rows,
        group_id,
        RoutingTarget::Backend(BackendRef::Group(record.backend_id)),
    );
    let named = BucketRules::Named("b".to_string());
    let snapshot = run(
        SnapshotOperation::new(&NodeRouting::default(), group_id, named),
        &rows,
    )
    .unwrap();
    assert_eq!(
        resolve_backend(&snapshot, "b", "k").unwrap(),
        ResolvedBackend::new(BackendRef::Group(record.backend_id), None)
    );

    let other = Ulid::from_parts(2, 2);
    with_default(
        &mut rows,
        other,
        RoutingTarget::Backend(BackendRef::Group(record.backend_id)),
    );
    let named = BucketRules::Named("b".to_string());
    let foreign = run(
        SnapshotOperation::new(&NodeRouting::default(), other, named),
        &rows,
    )
    .unwrap();
    assert!(resolve_backend(&foreign, "b", "k").is_err());
}

#[test]
fn reads_bucket_rules() {
    // Stored and known bucket records give the same rules and compression; an absent one is empty.
    let group_id = Ulid::from_parts(1, 1);
    let rule = StorageRoutingRule {
        key_prefix: "archive/".to_string(),
        exact: false,
        target: RoutingTarget::Class("cold".to_string()),
    };
    let info = bucket(group_id, vec![rule.clone()]);
    let mut rows = Rows::new();
    rows.insert(
        (S3_BUCKET_KEYSPACE.to_string(), b"routed".to_vec()),
        info.to_bytes().unwrap(),
    );
    let routing = NodeRouting::default();
    let named = BucketRules::Named("routed".to_string());
    let stored = run(SnapshotOperation::new(&routing, group_id, named), &rows).unwrap();
    let known = run(
        SnapshotOperation::new(&routing, group_id, BucketRules::of(&info)),
        &rows,
    )
    .unwrap();
    assert_eq!(stored, known);
    assert_eq!(stored.bucket_rules, vec![rule]);
    assert_eq!(stored.compression, info.compression);

    let named = BucketRules::Named("missing".to_string());
    let absent = run(SnapshotOperation::new(&routing, group_id, named), &rows).unwrap();
    assert!(absent.bucket_rules.is_empty());
    assert_eq!(absent.group_default, None);
}

#[test]
fn snapshot_calls_order() {
    // Bucket read, the group's inputs as a child, then one read of the capped backend's shards.
    let group_id = Ulid::from_parts(1, 1);
    let named = BucketRules::Named("b".to_string());
    let mut operation = SnapshotOperation::new(&capped(10), group_id, named);
    let bucket = (S3_BUCKET_KEYSPACE.to_string(), Key::from(b"b".to_vec()));
    assert_eq!(
        operation.first(),
        Ok(Next::Call(IoCall::Read(vec![bucket])))
    );
    let inputs = operation.next(IoResult::Values(vec![None]));
    assert!(matches!(inputs, Ok(Next::Call(IoCall::Child(_)))));
    let loaded = Ok::<_, GroupInputsError>(GroupRoutingInputs::default());
    let usage = operation.next(IoResult::Child(ChildOutcome::new(loaded)));
    assert_eq!(
        usage,
        Ok(Next::Call(usage_read(&[("cold".to_string(), 10)])))
    );
    let shards = usage_backend_keys(&BackendRef::Node("cold".to_string())).len();
    let counters = operation.next(IoResult::Values(vec![None; shards]));
    assert!(matches!(counters, Ok(Next::Done(_))));
}

#[test]
fn marks_full_backend() {
    // Fullness sums every shard of the capped backend; under the quota it stays open.
    let mut rows = Rows::new();
    with_usage(&mut rows, "cold", 0, 6);
    with_usage(&mut rows, "cold", 5, 4);
    let full = run(QuotaOperation::new(capped(10)), &rows).unwrap();
    assert_eq!(full.catalog, capped(10).catalog.mark_full("cold"));
    let open = run(QuotaOperation::new(capped(11)), &rows).unwrap();
    assert_eq!(open.catalog, capped(11).catalog);
    // An uncapped node reads nothing at all.
    let mut operation = QuotaOperation::new(NodeRouting::default());
    assert_eq!(operation.first(), Ok(Next::Done(NodeRouting::default())));
}

#[test]
fn sums_backend_shards() {
    // A missing row reads as zero, one undecodable shard fails the read.
    let mut rows = Rows::new();
    with_usage(&mut rows, BackendRef::DEFAULT_NODE_NAME, 0, 10);
    with_usage(&mut rows, BackendRef::DEFAULT_NODE_NAME, 5, 7);
    with_usage(&mut rows, "cold", 0, 100);
    assert_eq!(
        run(UsageOperation::new(BackendRef::node_default()), &rows),
        Ok(17)
    );
    let gone = BackendRef::Node("gone".to_string());
    assert_eq!(run(UsageOperation::new(gone), &rows), Ok(0));
    let shard = usage_backend_key(&BackendRef::node_default(), 1);
    rows.insert((USAGE_STATS_KEYSPACE.to_string(), shard), vec![0xff; 8]);
    assert!(run(UsageOperation::new(BackendRef::node_default()), &rows).is_err());
}

#[test]
fn failures_end_operations() {
    // A stray answer, a corrupt bucket and a short batch each fail instead of routing.
    let named = || BucketRules::Named("b".to_string());
    let mut operation = SnapshotOperation::new(&NodeRouting::default(), Ulid::nil(), named());
    operation.first().unwrap();
    assert_eq!(
        operation.next(IoResult::Done),
        Err(RoutingInputsError::BucketRules(RecordReadError::Unexpected))
    );

    let mut rows = Rows::new();
    rows.insert(
        (S3_BUCKET_KEYSPACE.to_string(), b"b".to_vec()),
        vec![0xff; 8],
    );
    let corrupt = SnapshotOperation::new(&NodeRouting::default(), Ulid::nil(), named());
    assert!(matches!(
        run(corrupt, &rows),
        Err(RoutingInputsError::BucketRules(_))
    ));

    let mut quota = QuotaOperation::new(capped(10));
    quota.first().unwrap();
    assert_eq!(
        quota.next(IoResult::Values(Vec::new())),
        Err(RoutingInputsError::BackendUsage(
            RecordReadError::Unexpected
        ))
    );
    let mut usage = UsageOperation::new(BackendRef::node_default());
    usage.first().unwrap();
    assert_eq!(usage.next(IoResult::Done), Err(RecordReadError::Unexpected));
}
