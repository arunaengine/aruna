//! Tests of the shard restore operations and of recovery runs on real nodes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use super::*;
use crate::driver::{DriverContext, drive};
use crate::shard::restore_plan::{
    RESTORE_CHUNK_TOPICS, RESTORE_TOPIC_COUNT, RESTORE_UNIT_BUDGET, shared_topic_peers,
};
use aruna_core::UserId;
use aruna_core::calls::ChildOutcome;
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, NetEffect, StorageEffect};
use aruna_core::events::{Event, NetEvent, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::structs::identity::realm::{RealmNode, RealmNodeKind};
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::placement::record::PlacementStrategy;
use aruna_storage::storage::FjallStorage;
use tempfile::tempdir;
use tokio_util::sync::CancellationToken;

const RECOVERY_TEST_GUARD: Duration = Duration::from_secs(5 * 60);
/// Real-time grace before a pass that published nothing counts as stuck.
const RECOVERY_PASS_GRACE: Duration = Duration::from_secs(5);

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn sharded_config(nodes: &[NodeId], shards: u32) -> RealmConfigDocument {
    let mut config = RealmConfigDocument::new(RealmId([7; 32]), Vec::new(), 3);
    let strategy = PlacementStrategy {
        strategy_id: ulid::Ulid::from_bytes([5u8; 16]),
        name: "default".to_string(),
        replica_count: None,
        distinct_locations: false,
        affinity: Vec::new(),
        shard_count: shards,
    };
    config.default_strategy_id = Some(strategy.strategy_id);
    config.strategies = vec![strategy];
    for node_id in nodes {
        config.ensure_node(*node_id, RealmNodeKind::Server);
    }
    config
}

// Every node chooses the lowest sync-eligible realm minter.
// A lower device id must never mint a competing genesis.
#[test]
fn realm_minter_agrees() {
    let realm_id = RealmId([7; 32]);
    let mut ids = [node(1), node(2), node(3)];
    ids.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    let (user, minter, follower) = (ids[0], ids[1], ids[2]);
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 2);
    config.ensure_node(
        user,
        RealmNodeKind::User {
            owner: UserId::nil(realm_id),
        },
    );
    config.ensure_node(minter, RealmNodeKind::Server);
    config.ensure_node(follower, RealmNodeKind::Server);

    let mints_realm_topics = |node_id| {
        plan_shard_groups(&config, node_id, realm_id, 0)
            .into_units()
            .iter()
            .any(|unit| {
                unit.kind == RestoreKind::Shared
                    && unit.topics.contains(
                        &DocumentTarget::RealmConfig { realm_id }
                            .sync_topic_id(realm_id, &PlacementRef::NIL),
                    )
            })
    };
    assert!(mints_realm_topics(minter));
    assert!(!mints_realm_topics(follower));
    assert!(
        !mints_realm_topics(user),
        "a node outside the sync-eligible set is never the realm minter"
    );
}

// Devices may remain offline, so restore excludes them from blocking peer sets.
// They receive separate best-effort pushes.
#[test]
fn units_skip_devices() {
    let realm_id = RealmId([7; 32]);
    let (server, other, device) = (node(1), node(2), node(3));
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 2);
    config.ensure_node(server, RealmNodeKind::Server);
    config.ensure_node(other, RealmNodeKind::Server);
    config.ensure_node(
        device,
        RealmNodeKind::User {
            owner: UserId::nil(realm_id),
        },
    );

    for unit in plan_shard_groups(&config, server, realm_id, 0).into_units() {
        assert!(
            !unit.peers.contains(&device),
            "{:?} targets a device",
            unit.kind
        );
    }
    // The device plans no realm-wide topic and reaches nobody with the ones
    // that are its own.
    let planned = plan_shard_groups(&config, device, realm_id, 0);
    assert_eq!(planned.summary.shared_topics, RESTORE_TOPIC_COUNT - 2);
    assert!(
        planned
            .into_units()
            .iter()
            .all(|unit| unit.peers.is_empty()),
        "a device reaches no peer for its own topics"
    );
}

#[test]
fn restore_units_chunk() {
    let local = node(1);
    let config = sharded_config(&[local, node(2), node(3)], 256);
    let plan = plan_shard_groups(&config, local, RealmId([7; 32]), 0);
    let held = plan.summary.held_shards;
    let units = plan.into_units();

    assert!(held > 0, "the local node should hold shards");
    assert!(
        units
            .iter()
            .all(|unit| unit.topics.len() <= RESTORE_CHUNK_TOPICS),
        "a work unit must never exceed its topic chunk"
    );
    // Grouping keeps a restart at one unit per topic chunk, never one per
    // held shard.
    assert!(
        units.len() < held,
        "{} units for {held} held shards is not batched",
        units.len()
    );
}

// One pass never exceeds its work-unit limit; the cursor resumes after the
// last processed unit and wraps at the end so no group is skipped forever.
#[test]
fn restore_cursor_wraps() {
    let units_total = RESTORE_UNIT_BUDGET * 2 + 3;
    let mut cursor = ShardRestoreCursor::default();
    let mut visited = Vec::new();
    for _ in 0..3 {
        let RestoreRange {
            start,
            end,
            wrapped,
        } = restore_range(cursor, units_total);
        assert!(end - start <= RESTORE_UNIT_BUDGET);
        visited.extend(start..end);
        cursor.next_unit = if wrapped { 0 } else { end };
        if wrapped {
            break;
        }
    }
    assert_eq!(visited, (0..units_total).collect::<Vec<_>>());
    assert_eq!(cursor, ShardRestoreCursor::default(), "cursor must wrap");
}

#[test]
fn cursor_rebinds_plan() {
    let topic = |seed| ::irokle::TopicId::from_bytes([seed; 32]);
    let unit = |seed| RestoreUnit {
        publishers: Vec::new(),
        kind: RestoreKind::Shared,
        peers: vec![node(seed)],
        retained: BTreeSet::new(),
        topics: vec![topic(seed)],
    };
    let mut cursor = ShardRestoreCursor::default();
    let original = vec![unit(1), unit(2), unit(3)];

    assert!(bind_restore_plan(&mut cursor, &original));
    cursor.next_unit = 2;
    assert!(!bind_restore_plan(&mut cursor, &original));
    assert_eq!(cursor.next_unit, 2);

    let mut reordered = original.clone();
    reordered.swap(0, 2);
    assert!(bind_restore_plan(&mut cursor, &reordered));
    assert_eq!(cursor.next_unit, 0);

    cursor.next_unit = 2;
    assert!(bind_restore_plan(&mut cursor, &original[..2]));
    assert_eq!(cursor.next_unit, 0);

    cursor.next_unit = 1;
    let mut added = original;
    added.push(unit(4));
    assert!(bind_restore_plan(&mut cursor, &added));
    assert_eq!(cursor.next_unit, 0);
}

#[test]
fn cursor_rebinds_config() {
    let local = node(1);
    let mut config = sharded_config(&[local, node(2), node(3)], 16);
    let original = plan_shard_groups(&config, local, RealmId([7; 32]), 0).into_units();
    let mut cursor = ShardRestoreCursor::default();
    assert!(bind_restore_plan(&mut cursor, &original));
    cursor.next_unit = 1;

    config.strategies[0].shard_count = 32;
    let changed = plan_shard_groups(&config, local, RealmId([7; 32]), 0).into_units();
    assert!(bind_restore_plan(&mut cursor, &changed));
    assert_eq!(cursor.next_unit, 0);
}

fn assert_rebind(before: &RealmConfigDocument, after: &RealmConfigDocument, expected_change: bool) {
    let local = node(1);
    let realm_id = RealmId([7; 32]);
    let before_units = plan_shard_groups(before, local, realm_id, 0).into_units();
    let after_units = plan_shard_groups(after, local, realm_id, 0).into_units();
    let mut cursor = ShardRestoreCursor::default();
    assert!(bind_restore_plan(&mut cursor, &before_units));
    cursor.next_unit = 1;

    assert_eq!(
        bind_restore_plan(&mut cursor, &after_units),
        expected_change
    );
    assert_eq!(cursor.next_unit, if expected_change { 0 } else { 1 });
}

#[test]
fn config_rebinds_matrix() {
    let local = node(1);
    let peer_one = node(2);
    let peer_two = node(3);
    let base = sharded_config(&[local, peer_one, peer_two], 16);

    let mut add_strategy = base.clone();
    add_strategy.strategies.push(PlacementStrategy {
        strategy_id: ulid::Ulid::from_bytes([6u8; 16]),
        name: "added".to_string(),
        replica_count: None,
        distinct_locations: false,
        affinity: Vec::new(),
        shard_count: 8,
    });
    assert_rebind(&base, &add_strategy, true);

    let mut add_group = base.clone();
    add_group.ensure_node(node(4), RealmNodeKind::Server);
    assert_rebind(&base, &add_group, true);

    let mut shrink = base.clone();
    shrink.strategies[0].shard_count = 8;
    assert_rebind(&base, &shrink, true);

    let mut remove = base.clone();
    remove.strategies.clear();
    remove.default_strategy_id = None;
    assert_rebind(&base, &remove, true);

    let mut reorder_base = base.clone();
    reorder_base.strategies.push(PlacementStrategy {
        strategy_id: ulid::Ulid::from_bytes([7u8; 16]),
        name: "second".to_string(),
        replica_count: None,
        distinct_locations: false,
        affinity: Vec::new(),
        shard_count: 8,
    });
    let mut reorder = reorder_base.clone();
    reorder.strategies.reverse();
    assert_rebind(&reorder_base, &reorder, true);

    let mut rebind = base.clone();
    let strategy_id = ulid::Ulid::from_bytes([8u8; 16]);
    rebind.strategies[0].strategy_id = strategy_id;
    rebind.default_strategy_id = Some(strategy_id);
    assert_rebind(&base, &rebind, true);

    let mut description = base.clone();
    description.description = "metadata-only edit".to_string();
    assert_rebind(&base, &description, false);

    let mut rename = base.clone();
    rename.strategies[0].name = "renamed".to_string();
    assert_rebind(&base, &rename, false);

    let mut add_user = base.clone();
    add_user.ensure_node(
        node(4),
        RealmNodeKind::User {
            owner: UserId::nil(RealmId([7; 32])),
        },
    );
    assert_rebind(&base, &add_user, false);
}

fn flat_config(realm_id: RealmId, nodes: &[NodeId]) -> RealmConfigDocument {
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 2);
    for node_id in nodes {
        config.ensure_node(*node_id, RealmNodeKind::Server);
    }
    config
}

#[tokio::test]
async fn bad_config_degrades() {
    let dir = tempdir().expect("tempdir must open");
    let storage =
        FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage must open");
    let realm_id = RealmId([8; 32]);
    let target = DocumentTarget::RealmConfig { realm_id };
    let event = storage
        .send_storage_effect(StorageEffect::Write {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key(),
            value: vec![0xff].into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
    let context = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    });
    // A failed config read must not unbind the cursor from its plan.
    let bound = ShardRestoreCursor {
        next_unit: 3,
        plan_hash: Some([7; 32]),
    };
    let restore = RestorePassOperation::new(node(1), realm_id, bound, RecoveryStop::default());
    let (pass, cursor) = drive(restore, &context).await.expect("pass runs");
    assert_eq!(pass.error, Some(RecoveryError::Storage));
    assert!(pass.wrapped);
    assert!(!pass.enumerated);
    assert_eq!(cursor, bound);
}

#[test]
fn shared_peers_filter() {
    let self_id = node(1);
    let management = node(2);
    let server = node(3);
    let user = node(5);
    let realm_id = RealmId([9; 32]);
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.nodes = vec![
        RealmNode {
            node_id: self_id.to_string(),
            kind: RealmNodeKind::Management,
        },
        RealmNode {
            node_id: management.to_string(),
            kind: RealmNodeKind::Management,
        },
        RealmNode {
            node_id: user.to_string(),
            kind: RealmNodeKind::User {
                owner: UserId::nil(realm_id),
            },
        },
        RealmNode {
            node_id: "malformed-eligible-id".to_string(),
            kind: RealmNodeKind::Server,
        },
        RealmNode {
            node_id: server.to_string(),
            kind: RealmNodeKind::Server,
        },
    ];

    assert_eq!(
        shared_topic_peers(&config, self_id),
        vec![management, server]
    );
}

fn topic(seed: u8) -> TopicId {
    TopicId::from_bytes([seed; 32])
}

fn answer(event: ShardNetEvent) -> IoResult {
    let event = Event::Net(NetEvent::Shard(Box::new(event)));
    IoResult::Event(Box::new(event))
}

fn existing(topics: &[TopicId]) -> IoResult {
    answer(ShardNetEvent::Existing(topics.iter().copied().collect()))
}

fn outcome(result: Result<(), &str>) -> IoResult {
    answer(ShardNetEvent::Outcome(result.map_err(str::to_string)))
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn pulled(applied: bool) -> IoResult {
    child::<bool, IoError>(Ok(applied))
}

fn work_unit(kind: RestoreKind, peers: Vec<NodeId>, topics: Vec<TopicId>) -> RestoreUnit {
    RestoreUnit {
        kind,
        peers,
        publishers: vec![node(1)],
        retained: BTreeSet::new(),
        topics,
    }
}

fn unit_operation(unit: RestoreUnit) -> RestoreUnitOperation {
    RestoreUnitOperation::new(node(1), unit, BTreeSet::new())
}

fn pull_call(topics: Vec<TopicId>, peers: Vec<NodeId>) -> IoCall {
    IoCall::child(PullTopicsOperation::new(topics, peers))
}

fn resolved(to_ensure: Vec<TopicId>, withheld: bool) -> IoResult {
    child::<_, IoError>(Ok(ResolvedTopics {
        to_ensure,
        withheld,
        unreachable: BTreeSet::new(),
    }))
}

fn unresolved(topics: &[TopicId]) -> RestoreUnitOutcome {
    let mut outcome = RestoreUnitOutcome::default();
    outcome.fail_topics(topics.iter().copied());
    outcome
}

#[test]
fn lone_unit_mints() {
    let unit = work_unit(RestoreKind::Shared, Vec::new(), vec![topic(1)]);
    let mut restore = unit_operation(unit);
    let resolve = ResolveTopicsOperation::new(Vec::new(), vec![topic(1)], true, BTreeSet::new());
    assert_eq!(restore.first(), Ok(Next::Call(IoCall::child(resolve))));
    let ensure = shard_call(ShardNetEffect::EnsureLocal(vec![topic(1)]));
    let turn = restore.next(resolved(vec![topic(1)], false));
    assert_eq!(turn, Ok(Next::Call(ensure)));
    let turn = restore.next(outcome(Err("refused")));
    assert_eq!(turn, Ok(Next::Done(unresolved(&[topic(1)]))));
}

#[test]
fn join_never_mints() {
    let unit = work_unit(RestoreKind::SharedJoin, vec![node(2)], vec![topic(1)]);
    let mut restore = unit_operation(unit);
    let resolve =
        ResolveTopicsOperation::new(vec![node(2)], vec![topic(1)], false, BTreeSet::new());
    assert_eq!(restore.first(), Ok(Next::Call(IoCall::child(resolve))));
}

#[test]
fn shared_unit_pulls() {
    let unit = work_unit(RestoreKind::Shared, vec![node(2)], vec![topic(1), topic(2)]);
    let mut restore = unit_operation(unit);
    restore.first().unwrap();
    let ensure = shard_call(ShardNetEffect::EnsureShared {
        topics: vec![topic(1)],
        peers: vec![node(2)],
    });
    let turn = restore.next(resolved(vec![topic(1)], true));
    assert_eq!(turn, Ok(Next::Call(ensure)));
    // A failed ensure still pulls: a peer may hold what could not be created here.
    let turn = restore.next(outcome(Err("refused")));
    assert_eq!(
        turn,
        Ok(Next::Call(pull_call(vec![topic(1)], vec![node(2)])))
    );
    let turn = restore.next(pulled(true));
    assert_eq!(turn, Ok(Next::Done(unresolved(&[topic(1), topic(2)]))));
}

#[test]
fn failed_resolve_withholds() {
    let unit = work_unit(RestoreKind::Shared, vec![node(2)], vec![topic(1)]);
    let mut restore = unit_operation(unit);
    restore.first().unwrap();
    let turn = restore.next(child::<ResolvedTopics, _>(Err(IoError::Aborted)));
    assert_eq!(turn, Ok(Next::Done(unresolved(&[topic(1)]))));
}

#[test]
fn rank_zero_pulls() {
    let topics = vec![topic(1), topic(2)];
    let unit = work_unit(RestoreKind::Rank0, vec![node(2)], topics.clone());
    let mut restore = unit_operation(unit);
    assert!(matches!(restore.first(), Ok(Next::Call(IoCall::Child(_)))));
    let genesis = child::<_, IoError>(Ok((true, BTreeSet::<NodeId>::new())));
    let turn = restore.next(genesis);
    let check = shard_call(ShardNetEffect::Existing(topics));
    assert_eq!(turn, Ok(Next::Call(check)));
    let turn = restore.next(existing(&[topic(1)]));
    assert_eq!(
        turn,
        Ok(Next::Call(pull_call(vec![topic(1)], vec![node(2)])))
    );
    let turn = restore.next(pulled(false));
    assert_eq!(turn, Ok(Next::Done(unresolved(&[topic(1), topic(2)]))));
}

#[test]
fn withheld_genesis_reported() {
    // Every topic exists, but the withheld genesis still counts as a peer problem.
    let unit = work_unit(RestoreKind::Rank0, vec![node(2)], vec![topic(1)]);
    let mut restore = unit_operation(unit);
    restore.first().unwrap();
    restore
        .next(child::<(bool, BTreeSet<NodeId>), _>(Err(IoError::Aborted)))
        .unwrap();
    restore.next(existing(&[topic(1)])).unwrap();
    let expected = RestoreUnitOutcome {
        unresolved_topics: BTreeSet::new(),
        error: Some(RecoveryError::PeerUnavailable),
    };
    assert_eq!(restore.next(pulled(true)), Ok(Next::Done(expected)));
}

#[test]
fn join_tops_members() {
    let topics = vec![topic(1), topic(2)];
    let unit = work_unit(RestoreKind::Join, vec![node(2)], topics.clone());
    let mut restore = unit_operation(unit.clone());
    let membership = |topics| restore_membership(&unit, topics);
    assert_eq!(restore.first(), Ok(Next::Call(membership(topics.clone()))));
    let turn = restore.next(outcome(Ok(())));
    assert_eq!(
        turn,
        Ok(Next::Call(pull_call(topics.clone(), vec![node(2)])))
    );
    let turn = restore.next(pulled(true));
    let check = shard_call(ShardNetEffect::Existing(topics));
    assert_eq!(turn, Ok(Next::Call(check)));
    let turn = restore.next(existing(&[topic(2)]));
    assert_eq!(turn, Ok(Next::Call(membership(vec![topic(2)]))));
    let turn = restore.next(outcome(Err("refused")));
    assert_eq!(turn, Ok(Next::Done(unresolved(&[topic(1), topic(2)]))));
}

fn restore_membership(unit: &RestoreUnit, topics: Vec<TopicId>) -> IoCall {
    let group = (
        unit.peers.clone(),
        unit.publishers.clone(),
        unit.retained.clone(),
    );
    membership_call(topics, &group, node(1), &BTreeSet::new())
}

fn pass_operation(stop: RecoveryStop) -> RestorePassOperation {
    let cursor = ShardRestoreCursor::default();
    RestorePassOperation::new(node(1), RealmId([7; 32]), cursor, stop)
}

/// A pass over a two-node flat realm that has read its verified topics.
fn started_pass(stop: RecoveryStop) -> (RestorePassOperation, Result<Next<RestoredPass>, IoError>) {
    let mut pass = pass_operation(stop);
    let config = flat_config(RealmId([7; 32]), &[node(1), node(2)]);
    pass.first().unwrap();
    let turn = pass.next(child::<_, GetConfigError>(Ok(config))).unwrap();
    assert_eq!(
        turn,
        Next::Call(shard_call(ShardNetEffect::Existing(Vec::new())))
    );
    pass.next(existing(&[])).unwrap();
    let turn = pass.next(child::<_, IoError>(Ok(BTreeSet::<TopicId>::new())));
    (pass, turn)
}

#[test]
fn missing_config_wraps() {
    let mut pass = pass_operation(RecoveryStop::default());
    pass.first().unwrap();
    let missing = child::<RealmConfigDocument, _>(Err(GetConfigError::DocumentNotFound));
    let Ok(Next::Done((done, cursor))) = pass.next(missing) else {
        panic!("a missing config ends the pass");
    };
    assert!(done.wrapped && done.enumerated);
    assert_eq!(cursor, ShardRestoreCursor::default());
}

#[test]
fn no_network_wraps() {
    let mut pass = pass_operation(RecoveryStop::default());
    pass.first().unwrap();
    let config = flat_config(RealmId([7; 32]), &[node(1)]);
    pass.next(child::<_, GetConfigError>(Ok(config))).unwrap();
    let Ok(Next::Done((done, _))) = pass.next(answer(ShardNetEvent::NoNetwork)) else {
        panic!("no net transport ends the pass");
    };
    assert!(done.wrapped && done.enumerated);
    assert_eq!(done.units_total, 0);
}

#[test]
fn stop_keeps_cursor() {
    let token = CancellationToken::new();
    token.cancel();
    let (_, turn) = started_pass(RecoveryStop::new(token));
    let Ok(Next::Done((done, cursor))) = turn else {
        panic!("a stopped pass runs no unit");
    };
    assert_eq!(done.units_processed, 0);
    assert!(!done.wrapped);
    assert_eq!(cursor.next_unit, 0);
    assert!(cursor.plan_hash.is_some());
}

#[test]
fn withheld_arms_retry() {
    let (mut pass, turn) = started_pass(RecoveryStop::default());
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let topics = pass.units[pass.position].topics.clone();
    let mut turn = pass.next(child::<_, IoError>(Ok(unresolved(&topics))));
    while pass.stage == PassStage::Unit {
        turn = pass.next(child::<_, IoError>(Ok(RestoreUnitOutcome::default())));
    }
    let retry = schedule_retry_effect(RealmId([7; 32]), node(1));
    assert_eq!(turn, Ok(Next::Call(IoCall::from(retry))));
    let verify = VerifyHeldOperation::new(node(1), RealmId([7; 32]));
    let turn = pass.next(IoResult::Done);
    assert_eq!(turn, Ok(Next::Call(IoCall::child(verify))));
    let summary = ShardVerificationSummary::default();
    let Ok(Next::Done((done, cursor))) = pass.next(child::<_, IoError>(Ok(summary))) else {
        panic!("verification ends the pass");
    };
    assert!(done.wrapped);
    assert_eq!(done.unresolved_topics, topics.into_iter().collect());
    assert_eq!(done.error, Some(RecoveryError::PeerUnavailable));
    assert_eq!(cursor.next_unit, 0);
}

#[test]
fn stopped_unit_unresolved() {
    let (mut pass, _) = started_pass(RecoveryStop::default());
    let topics = pass.units[pass.position].topics.clone();
    pass.next(child::<RestoreUnitOutcome, _>(Err(IoError::Aborted)))
        .unwrap();
    assert!(topics.iter().all(|topic| pass.unresolved.contains(topic)));
}

#[test]
fn topics_until_wrapped() {
    let mut restore = RestoreTopicsOperation::new(node(1), RealmId([7; 32]));
    let pass_at = |cursor| {
        IoCall::child(RestorePassOperation::new(
            node(1),
            RealmId([7; 32]),
            cursor,
            RecoveryStop::default(),
        ))
    };
    let start = ShardRestoreCursor::default();
    assert_eq!(restore.first(), Ok(Next::Call(pass_at(start))));
    let mut first = ShardRestorePass::default();
    first.summary.withheld_topics = 2;
    let resume = ShardRestoreCursor {
        next_unit: 4,
        plan_hash: Some([1; 32]),
    };
    let turn = restore.next(child::<RestoredPass, IoError>(Ok((first, resume))));
    assert_eq!(turn, Ok(Next::Call(pass_at(resume))));
    let mut last = ShardRestorePass {
        wrapped: true,
        ..ShardRestorePass::default()
    };
    last.summary.withheld_topics = 1;
    last.summary.held_shards = 3;
    let turn = restore.next(child::<RestoredPass, IoError>(Ok((last, start))));
    let expected = RestoreShardSummary {
        held_shards: 3,
        withheld_topics: 3,
        ..RestoreShardSummary::default()
    };
    assert_eq!(turn, Ok(Next::Done(expected)));
}

#[test]
fn policy_needs_network() {
    let mut prepare = PreparePolicyOperation::new(node(1), RealmId([7; 32]));
    let check = shard_call(ShardNetEffect::Existing(Vec::new()));
    assert_eq!(prepare.first(), Ok(Next::Call(check)));
    let turn = prepare.next(answer(ShardNetEvent::NoNetwork));
    assert_eq!(turn, Ok(Next::Done(RestoreShardSummary::default())));
}

#[test]
fn policy_covers_present() {
    let realm_id = RealmId([7; 32]);
    let config = sharded_config(&[node(1), node(2)], 4);
    let mut prepare = PreparePolicyOperation::new(node(1), realm_id);
    prepare.first().unwrap();
    prepare.next(existing(&[])).unwrap();
    prepare
        .next(child::<_, GetConfigError>(Ok(config)))
        .unwrap();
    let turn = prepare.next(child::<_, IoError>(Ok(BTreeSet::<TopicId>::new())));
    let Ok(Next::Call(check)) = turn else {
        panic!("the first group is checked");
    };
    let IoCall::Effect(effect) = check else {
        panic!("the check is a shard effect");
    };
    let Effect::Net(NetEffect::Shard(shard)) = *effect else {
        panic!("the check is a shard effect");
    };
    let ShardNetEffect::Existing(topics) = *shard else {
        panic!("the check lists existing topics");
    };
    let turn = prepare.next(existing(&topics[..1]));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Effect(_)))));
    assert_eq!(prepare.stage, PolicyStage::Members);
    assert_eq!(prepare.prepared, 1);
}
