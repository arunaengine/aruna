//! Tests of the shard restore operations and of recovery runs on real nodes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use super::*;
use crate::driver::{DriverContext, drive};
use crate::node::recovery_run::{
    PresencePhaseOperation, RECOVERY_RETRY_BASE, RECOVERY_RETRY_MAX, RecoveryConfig,
    RecoveryOperation,
};
use crate::placement::process_placements::ReconcilePlacementsOperation;
use crate::shard::recovery::{RecoveryOutcome, RecoveryState, RecoveryStatus};
use crate::shard::restore_plan::{
    RESTORE_CHUNK_TOPICS, RESTORE_TOPIC_COUNT, RESTORE_UNIT_BUDGET, shared_topic_peers,
};
use aruna_core::UserId;
use aruna_core::calls::ChildOutcome;
use aruna_core::document::{DocumentTarget, shard_topic_id};
use aruna_core::effects::{DhtEffect, DhtGetOptions, Effect, NetEffect, StorageEffect};
use aruna_core::events::{DhtEntry, DhtEvent, Event, NetEvent, StorageEvent};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::{RealmNode, RealmNodeKind};
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::placement::record::{PlacementOverride, PlacementStrategy};
use aruna_net::{NetConfig, NetHandle};
use aruna_storage::storage::FjallStorage;
use aruna_tasks::TaskHandle;
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

struct RecoveryNode {
    _dir: tempfile::TempDir,
    net: NetHandle,
    task_handle: TaskHandle,
    context: Arc<DriverContext>,
}

async fn make_node(realm_id: RealmId, seed: u8) -> RecoveryNode {
    let dir = tempdir().expect("tempdir must open");
    let storage =
        FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage must open");
    let net = NetHandle::new(
        NetConfig {
            secret_key: Some(iroh::SecretKey::from_bytes(&[seed; 32])),
            realm_id,
            sync_storage_path: Some(dir.path().join("document-sync")),
            ..crate::testing::local_net()
        },
        storage.clone(),
    )
    .await
    .expect("net must open");
    let task_handle = TaskHandle::new();
    let context = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: Some(net.clone()),
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: Some(task_handle.clone()),
        compute_handle: None,
        outbound_handle: None,
    });
    crate::sync::incoming::initialize_incoming_fixture(context.clone());
    RecoveryNode {
        _dir: dir,
        net,
        task_handle,
        context,
    }
}

fn test_config(
    realm_id: RealmId,
    local: NodeId,
    peers: &[NodeId],
) -> (RealmConfigDocument, Vec<PlacementRef>) {
    let strategy_id = ulid::Ulid::from_bytes([6u8; 16]);
    let strategy = PlacementStrategy {
        strategy_id,
        name: "recovery-test".to_string(),
        replica_count: Some(2),
        distinct_locations: false,
        affinity: Vec::new(),
        shard_count: peers.len() as u32,
    };
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 2);
    config.default_strategy_id = Some(strategy_id);
    config.strategies = vec![strategy];
    config.ensure_node(local, RealmNodeKind::Server);
    for peer in peers {
        config.ensure_node(*peer, RealmNodeKind::Server);
    }

    let mut placements = Vec::with_capacity(peers.len());
    for (shard, peer) in peers.iter().enumerate() {
        let placement = PlacementRef {
            strategy_id,
            shard: shard as u32,
        };
        config.placement_overrides.push(PlacementOverride {
            subject: aruna_core::placement::shard_subject_bytes(&placement),
            pinned: vec![local, *peer],
            excluded: Vec::new(),
            strategy_id: None,
        });
        placements.push(placement);
    }
    (config, placements)
}

fn flat_config(realm_id: RealmId, nodes: &[NodeId]) -> RealmConfigDocument {
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 2);
    for node_id in nodes {
        config.ensure_node(*node_id, RealmNodeKind::Server);
    }
    config
}

async fn save_config(node: &RecoveryNode, config: &RealmConfigDocument) {
    let realm_id = config.realm_id;
    let target = DocumentTarget::RealmConfig { realm_id };
    let actor = Actor {
        node_id: node.net.node_id(),
        user_id: UserId::nil(realm_id),
        realm_id,
    };
    let event = node
        .context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key(),
            value: config.to_bytes(&actor).expect("config serializes").into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
    node.net
        .refresh_document_peers(config)
        .await
        .expect("realm peers refresh");
}

async fn mesh_nodes(first: &RecoveryNode, second: &RecoveryNode) {
    first.net.add_peer_addr(second.net.endpoint_addr()).await;
    second.net.add_peer_addr(first.net.endpoint_addr()).await;
}

async fn seed_topic(node: &RecoveryNode, topic: ::irokle::TopicId, peers: &[NodeId]) {
    node.net
        .ensure_sync_topics(&[topic], peers.to_vec())
        .await
        .expect("blocked peer topic creates");
}

fn spawn_driver(
    node: &RecoveryNode,
    realm_id: RealmId,
    publish_full_usage: bool,
) -> (
    RecoveryStatus,
    CancellationToken,
    tokio::task::JoinHandle<()>,
) {
    let status = RecoveryStatus::new();
    let cancelled = CancellationToken::new();
    let config = RecoveryConfig {
        realm_id,
        node_id: node.net.node_id(),
        publish_full_usage,
    };
    let stop = RecoveryStop::new(cancelled.clone());
    let recovery = RecoveryOperation::new(&node.context, config, status.clone(), stop);
    let context = node.context.clone();
    let driver = tokio::spawn(async move {
        drive(recovery, &context).await.expect("recovery runs");
    });
    (status, cancelled, driver)
}

/// Holds virtual time still while a pass runs, so deadlines guarding real
/// work inside it cannot fire spuriously. A pass may itself wait on a
/// deadline, so the clock resumes once one has stalled for real seconds.
#[derive(Default)]
struct PassClock {
    running_since: Option<std::time::Instant>,
}

impl PassClock {
    async fn tick(&mut self, status: &RecoveryStatus, step: Duration) {
        tokio::task::yield_now().await;
        if status.snapshot().state == RecoveryState::Degraded {
            self.running_since = None;
            tokio::time::advance(step).await;
            return;
        }
        let since = *self
            .running_since
            .get_or_insert_with(std::time::Instant::now);
        if since.elapsed() >= RECOVERY_PASS_GRACE {
            tokio::time::advance(step).await;
        }
    }
}

async fn wait_state(status: &RecoveryStatus, expected: RecoveryState) {
    let wait = async {
        let mut clock = PassClock::default();
        loop {
            if status.snapshot().state == expected {
                return;
            }
            clock.tick(status, Duration::from_millis(100)).await;
        }
    };
    tokio::time::timeout(RECOVERY_TEST_GUARD, wait)
        .await
        .unwrap_or_else(|_| panic!("recovery did not reach {expected:?}"));
}

async fn wait_partial(status: &RecoveryStatus, target: u64) {
    let wait = async {
        let mut clock = PassClock::default();
        loop {
            if status.pass_total(RecoveryOutcome::Partial) >= target {
                return;
            }
            clock.tick(status, Duration::from_millis(100)).await;
        }
    };
    tokio::time::timeout(RECOVERY_TEST_GUARD, wait)
        .await
        .unwrap_or_else(|_| panic!("recovery did not reach partial pass {target}"));
}

async fn wait_stall(status: &RecoveryStatus) -> (u64, u64, u64) {
    let wait = async {
        let mut observed = 0;
        let mut previous = None;
        let mut clock = PassClock::default();
        loop {
            let partial = status.pass_total(RecoveryOutcome::Partial);
            if partial <= observed || status.snapshot().state != RecoveryState::Degraded {
                clock.tick(status, Duration::from_millis(100)).await;
                continue;
            }
            clock.tick(status, RECOVERY_RETRY_BASE / 2).await;
            if status.snapshot().state == RecoveryState::Degraded
                && status.pass_total(RecoveryOutcome::Partial) == partial
            {
                observed = partial;
                let progress = status.progress_ms();
                let remaining = status.snapshot().topics_remaining;
                if previous == Some((progress, remaining)) {
                    return (partial, progress, remaining);
                }
                previous = Some((progress, remaining));
            }
        }
    };
    tokio::time::timeout(RECOVERY_TEST_GUARD, wait)
        .await
        .expect("recovery did not reach a stable retry")
}

async fn finish_driver(cancelled: CancellationToken, driver: tokio::task::JoinHandle<()>) {
    cancelled.cancel();
    driver.await.expect("recovery task must stop");
}

async fn stop_node(node: RecoveryNode) {
    let _ = node.task_handle.shutdown(RECOVERY_RETRY_MAX).await;
    node.net.shutdown().await;
}

async fn presence_values(node: &RecoveryNode, realm_id: RealmId) -> Vec<DhtEntry> {
    let event = node
        .net
        .send_effect(Effect::Net(NetEffect::Dht(DhtEffect::Get {
            key: aruna_core::keys::realm_presence_key(&realm_id),
            realm_filter: Some(realm_id),
            options: DhtGetOptions::exhaustive(RECOVERY_TEST_GUARD),
        })))
        .await;
    match event {
        Event::Net(NetEvent::Dht(DhtEvent::GetResult { values, .. })) => values,
        _ => Vec::new(),
    }
}

#[tokio::test]
async fn device_skips_presence() {
    // A User-kind node completes the presence phase without publishing; the
    // same node publishes once the realm config makes it a Server.
    let realm_id = RealmId([31u8; 32]);
    let device = make_node(realm_id, 31).await;
    let presence = || PresencePhaseOperation::new(realm_id, device.net.node_id());

    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 1);
    config.ensure_node(
        device.net.node_id(),
        RealmNodeKind::User {
            owner: UserId::nil(realm_id),
        },
    );
    save_config(&device, &config).await;

    let outcome = drive(presence(), &device.context)
        .await
        .expect("phase runs");
    assert!(outcome.progress);
    assert!(outcome.error.is_none());
    assert!(presence_values(&device, realm_id).await.is_empty());

    let mut promoted = RealmConfigDocument::new(realm_id, Vec::new(), 1);
    promoted.ensure_node(device.net.node_id(), RealmNodeKind::Server);
    save_config(&device, &promoted).await;

    let outcome = drive(presence(), &device.context)
        .await
        .expect("phase runs");
    assert!(outcome.progress);
    assert!(outcome.error.is_none());
    assert!(!presence_values(&device, realm_id).await.is_empty());

    stop_node(device).await;
}

#[tokio::test]
async fn restore_suffix() {
    let realm_id = RealmId([10; 32]);
    let local = make_node(realm_id, 1).await;
    let blocked = make_node(realm_id, 2).await;
    let healthy = make_node(realm_id, 3).await;
    let (config, placements) = test_config(
        realm_id,
        local.net.node_id(),
        &[blocked.net.node_id(), healthy.net.node_id()],
    );
    save_config(&local, &config).await;
    save_config(&blocked, &config).await;
    save_config(&healthy, &config).await;
    mesh_nodes(&local, &healthy).await;

    let blocked_topic = shard_topic_id(realm_id, &placements[0]);
    let healthy_topic = shard_topic_id(realm_id, &placements[1]);
    seed_topic(&blocked, blocked_topic, &[blocked.net.node_id()]).await;
    seed_topic(&healthy, healthy_topic, &[local.net.node_id()]).await;
    let restore = RestorePassOperation::new(
        local.net.node_id(),
        realm_id,
        ShardRestoreCursor::default(),
        RecoveryStop::default(),
    );
    let (pass, _) = drive(restore, &local.context).await.expect("pass runs");

    assert!(pass.wrapped);
    assert!(pass.unresolved_topics.contains(&blocked_topic));
    assert!(!pass.unresolved_topics.contains(&healthy_topic));
    assert!(
        local
            .net
            .sync_topic_exists(healthy_topic)
            .expect("healthy topic lookup")
    );
    assert!(pass.topics_completed > 0);

    stop_node(local).await;
    stop_node(blocked).await;
    stop_node(healthy).await;
}

#[tokio::test]
async fn heal_rotation() {
    let realm_id = RealmId([11; 32]);
    let local = make_node(realm_id, 4).await;
    let blocked = make_node(realm_id, 5).await;
    let (config, placements) = test_config(realm_id, local.net.node_id(), &[blocked.net.node_id()]);
    save_config(&local, &config).await;
    save_config(&blocked, &config).await;
    let topic = shard_topic_id(realm_id, &placements[0]);
    seed_topic(&blocked, topic, &[blocked.net.node_id()]).await;

    tokio::time::pause();
    let (status, cancelled, driver) = spawn_driver(&local, realm_id, false);
    wait_state(&status, RecoveryState::Degraded).await;
    assert!(status.snapshot().topics_remaining > 0);
    assert!(
        !local
            .net
            .sync_topic_exists(topic)
            .expect("local topic lookup")
    );

    mesh_nodes(&local, &blocked).await;
    // Every configured peer restores because only the designated minter creates shared topics.
    let restore = RestoreTopicsOperation::new(blocked.net.node_id(), realm_id);
    drive(restore, &blocked.context)
        .await
        .expect("restore runs");
    let reconcile = ReconcilePlacementsOperation::new(realm_id, blocked.net.node_id(), true);
    drive(reconcile, &blocked.context)
        .await
        .expect("reconcile runs");
    wait_state(&status, RecoveryState::Converged).await;
    assert_eq!(status.snapshot().state, RecoveryState::Converged);
    assert_eq!(status.snapshot().topics_remaining, 0);
    assert_eq!(status.snapshot().last_error, None);
    assert!(status.pass_total(RecoveryOutcome::Success) > 0);
    assert!(
        local
            .net
            .sync_topic_exists(topic)
            .expect("healed topic lookup")
    );

    finish_driver(cancelled, driver).await;
    stop_node(local).await;
    stop_node(blocked).await;
}

#[tokio::test]
async fn shared_retry() {
    let realm_id = RealmId([13; 32]);
    let local = make_node(realm_id, 8).await;
    let blocked = make_node(realm_id, 9).await;
    let config = flat_config(realm_id, &[local.net.node_id(), blocked.net.node_id()]);
    save_config(&local, &config).await;
    save_config(&blocked, &config).await;

    tokio::time::pause();
    let (status, cancelled, driver) = spawn_driver(&local, realm_id, false);
    wait_state(&status, RecoveryState::Degraded).await;
    assert_eq!(
        status.snapshot().last_error,
        Some(RecoveryError::PeerUnavailable)
    );
    assert!(status.snapshot().topics_remaining > 0);
    let partial = status.pass_total(RecoveryOutcome::Partial);

    wait_partial(&status, partial.saturating_add(1)).await;
    assert_eq!(status.snapshot().state, RecoveryState::Degraded);
    assert_eq!(
        status.snapshot().last_error,
        Some(RecoveryError::PeerUnavailable)
    );

    finish_driver(cancelled, driver).await;
    stop_node(local).await;
    stop_node(blocked).await;
}

#[tokio::test]
async fn usage_retry() {
    let realm_id = RealmId([14; 32]);
    let local = make_node(realm_id, 10).await;
    let config = flat_config(realm_id, &[local.net.node_id()]);
    save_config(&local, &config).await;
    let group_id = ulid::Ulid::from_bytes([15; 16]);
    let event = local
        .context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: aruna_core::keyspaces::USAGE_STATS_KEYSPACE.to_string(),
            key: aruna_core::structs::storage::usage::usage_group_key(group_id).into(),
            value: vec![0xff].into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));

    tokio::time::pause();
    let (status, cancelled, driver) = spawn_driver(&local, realm_id, true);
    wait_state(&status, RecoveryState::Degraded).await;
    assert_eq!(status.snapshot().last_error, Some(RecoveryError::Storage));
    assert_eq!(status.snapshot().topics_remaining, 0);
    let partial = status.pass_total(RecoveryOutcome::Partial);

    wait_partial(&status, partial.saturating_add(1)).await;
    assert_eq!(status.snapshot().state, RecoveryState::Degraded);
    assert_eq!(status.snapshot().last_error, Some(RecoveryError::Storage));
    assert_eq!(status.snapshot().topics_remaining, 0);

    finish_driver(cancelled, driver).await;
    stop_node(local).await;
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

#[tokio::test]
async fn stall_recovery() {
    let realm_id = RealmId([12; 32]);
    let local = make_node(realm_id, 6).await;
    let blocked = make_node(realm_id, 7).await;
    let (config, placements) = test_config(realm_id, local.net.node_id(), &[blocked.net.node_id()]);
    save_config(&local, &config).await;
    save_config(&blocked, &config).await;
    let topic = shard_topic_id(realm_id, &placements[0]);
    seed_topic(&blocked, topic, &[blocked.net.node_id()]).await;

    tokio::time::pause();
    let (status, cancelled, driver) = spawn_driver(&local, realm_id, false);
    let (partial, progress, remaining) = wait_stall(&status).await;
    // A stalled recovery must keep reporting its outstanding topics.
    assert!(remaining > 0);

    wait_partial(&status, partial.saturating_add(3)).await;
    assert_eq!(status.progress_ms(), progress);
    assert_eq!(status.snapshot().topics_remaining, remaining);

    finish_driver(cancelled, driver).await;
    stop_node(local).await;
    stop_node(blocked).await;
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
