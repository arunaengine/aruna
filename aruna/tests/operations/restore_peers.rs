//! Tests node restore between nodes that answer each other's peer requests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::IoResult;
use aruna_core::events::ShardNetEvent;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::identity::realm::RealmId;
use aruna_operations::shard::recovery::RecoveryError;
use aruna_operations::shard::recovery::RecoveryStop;
use aruna_operations::shard::restore::irokle::TopicId;
use aruna_operations::shard::restore_plan::ShardRestoreCursor;
use std::sync::Arc;
use std::time::Duration;

use aruna_core::UserId;
use aruna_core::document::{DocumentTarget, shard_topic_id};
use aruna_core::effects::{DhtEffect, DhtGetOptions, Effect, NetEffect, StorageEffect};
use aruna_core::events::{DhtEntry, DhtEvent, Event, NetEvent, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::RealmNodeKind;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::placement::record::{PlacementOverride, PlacementStrategy};
use aruna_net::{NetConfig, NetHandle};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::node::recovery_run::{
    PresencePhaseOperation, RECOVERY_RETRY_BASE, RECOVERY_RETRY_MAX, RecoveryConfig,
    RecoveryOperation,
};
use aruna_operations::placement::process_placements::ReconcilePlacementsOperation;
use aruna_operations::shard::recovery::{RecoveryOutcome, RecoveryState, RecoveryStatus};
use aruna_operations::shard::restore::*;
use aruna_storage::storage::FjallStorage;
use aruna_tasks::TaskHandle;
use tempfile::tempdir;
use tokio_util::sync::CancellationToken;

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
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
            ..aruna_operations::testing::local_net()
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
    aruna::inbound::initialize_incoming_fixture(context.clone());
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
    let serving = node.context.node_and_realm().map(|(_, realm_id)| realm_id);
    let recovery = RecoveryOperation::new(config, serving, status.clone(), stop);
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

fn topic(seed: u8) -> TopicId {
    TopicId::from_bytes([seed; 32])
}

fn answer(event: ShardNetEvent) -> IoResult {
    let event = Event::Net(NetEvent::Shard(Box::new(event)));
    IoResult::Event(Box::new(event))
}

fn outcome(result: Result<(), &str>) -> IoResult {
    answer(ShardNetEvent::Outcome(result.map_err(str::to_string)))
}
