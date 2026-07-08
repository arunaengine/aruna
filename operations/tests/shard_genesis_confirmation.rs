use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use aruna_core::document::shard_topic_id;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::structs::{Actor, PlacementRef, RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::{NodeId, UserId};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::driver::DriverContext;
use aruna_operations::incoming::initialize_net_incoming;
use aruna_operations::placement::resolve_shard_holders;
use aruna_operations::process_placements::process_shard_placements;
use aruna_operations::task_incoming::initialize_task_incoming;
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use irokle::oplog::Oplog;
use irokle::{ReplicationPolicy, TopicGenesis};
use tempfile::TempDir;
use tokio::time::sleep;

struct TestNode {
    _temp_dir: TempDir,
    net: NetHandle,
    context: Arc<DriverContext>,
}

// If the final ensure/create step fails after the topic was selected for
// ensuring, no pending placement record exists yet. The reconciler must still
// report retry-worthy work so the missing/invalid genesis path is retried.
#[tokio::test]
async fn rank0_topic_ensure_failure_schedules_retry() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([153u8; 32]);
    let nodes = build_realm_nodes(&realm_id, 2).await?;
    let config = install_realm_config(&nodes, realm_id).await?;
    mesh_nodes(&nodes).await;

    let (rank0, co_holder) = (&nodes[0], &nodes[1]);
    let placement = rank0_shard_of(&config, rank0.net.node_id(), co_holder.net.node_id());
    let topic = shard_topic_id(realm_id, &placement);
    seed_wrong_event_type_topic(&rank0.net, topic)?;

    let outcome = process_shard_placements(&rank0.context, realm_id, rank0.net.node_id()).await;

    assert!(
        outcome.retry_scheduled,
        "failed topic ensure must schedule a placement retry"
    );

    shutdown_nodes(nodes).await;
    Ok(())
}

// A reachable, topic-less co-holder is positive confirmation that no genesis
// exists: the rank-0 holder creates one.
#[tokio::test]
async fn reachable_topicless_co_holder_lets_rank0_create_the_genesis()
-> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([150u8; 32]);
    let nodes = build_realm_nodes(&realm_id, 2).await?;
    let config = install_realm_config(&nodes, realm_id).await?;
    mesh_nodes(&nodes).await;

    let (rank0, co_holder) = (&nodes[0], &nodes[1]);
    let placement = rank0_shard_of(&config, rank0.net.node_id(), co_holder.net.node_id());
    let topic = shard_topic_id(realm_id, &placement);
    assert!(
        !rank0.net.document_sync_topic_exists(topic).unwrap_or(false),
        "no genesis exists before the placement reconciler runs"
    );

    process_shard_placements(&rank0.context, realm_id, rank0.net.node_id()).await;

    assert!(
        rank0.net.document_sync_topic_exists(topic).unwrap_or(false),
        "an all-reached-none-know probe must let the rank-0 holder create the genesis"
    );

    shutdown_nodes(nodes).await;
    Ok(())
}

// An unreachable co-holder might hold a genesis, so creation is withheld — and,
// crucially, withholding schedules a SyncPlacements retry so a returning
// co-holder re-runs the reconciler on its own (no placement record exists to
// drive it). The retry is driven here through the real task handler rather than
// by re-running the reconciler by hand, which is what masked the liveness gap.
#[tokio::test]
async fn unreachable_co_holder_withholds_then_creates_on_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([151u8; 32]);
    let nodes = build_realm_nodes(&realm_id, 2).await?;
    let config = install_realm_config(&nodes, realm_id).await?;
    // Deliberately NOT meshed: the rank-0 holder has no path to the co-holder.

    let (rank0, co_holder) = (&nodes[0], &nodes[1]);
    let placement = rank0_shard_of(&config, rank0.net.node_id(), co_holder.net.node_id());
    let topic = shard_topic_id(realm_id, &placement);

    let outcome = process_shard_placements(&rank0.context, realm_id, rank0.net.node_id()).await;
    assert!(
        !rank0.net.document_sync_topic_exists(topic).unwrap_or(false),
        "an unreachable co-holder must withhold genesis creation (a fork would be permanent)"
    );
    assert!(
        outcome.retry_scheduled,
        "withholding must schedule a SyncPlacements retry so a returning co-holder re-runs the reconciler"
    );

    // The co-holder returns; the scheduled retry is fired immediately (instead of
    // after the 30s production delay) through the task handler. The test never
    // calls process_shard_placements itself past this point — convergence proves
    // the armed retry, not a hand-driven loop, re-creates the genesis.
    mesh_nodes(&nodes).await;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        fire_sync_placements_retry(rank0, realm_id).await;
        if rank0.net.document_sync_topic_exists(topic).unwrap_or(false) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "genesis was not created after the co-holder became reachable"
        );
        sleep(Duration::from_millis(200)).await;
    }

    shutdown_nodes(nodes).await;
    Ok(())
}

// Fires the SyncPlacements timer now (compressing the 30s production retry), so
// the task handler re-runs the reconciler through its real dispatch path.
async fn fire_sync_placements_retry(node: &TestNode, realm_id: RealmId) {
    let Some(task_handle) = node.context.task_handle.as_ref() else {
        return;
    };
    let _ = task_handle
        .send_effect(Effect::Task(TaskEffect::ResetTimer {
            key: TaskKey::SyncPlacements {
                realm_id,
                node_id: node.net.node_id(),
            },
            after: Duration::ZERO,
        }))
        .await;
}

// A co-holder that HAS the topic but has not yet admitted the prober silently
// omits its summary (irokle refuses the Open). A never-member rank-0 holder must
// read that as possibly-existing and withhold, not mint a forking genesis; once
// the co-holder's member top-up admits it, it adopts the existing genesis.
#[tokio::test]
async fn never_member_rank0_adopts_existing_genesis_after_top_up()
-> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([152u8; 32]);
    let nodes = build_realm_nodes(&realm_id, 2).await?;
    let config = install_realm_config(&nodes, realm_id).await?;
    mesh_nodes(&nodes).await;

    let (rank0, co_holder) = (&nodes[0], &nodes[1]);
    let placement = rank0_shard_of(&config, rank0.net.node_id(), co_holder.net.node_id());
    let topic = shard_topic_id(realm_id, &placement);

    // The co-holder holds the genesis with only itself as a member (passing its
    // own id leaves an empty sync-peer set), so it refuses the rank-0 holder's
    // probe and its summary is silently omitted.
    co_holder
        .net
        .ensure_document_sync_topics(&[topic], vec![co_holder.net.node_id()])?;
    assert!(
        co_holder
            .net
            .document_sync_topic_exists(topic)
            .unwrap_or(false),
        "the co-holder must hold the genesis before the probe"
    );

    process_shard_placements(&rank0.context, realm_id, rank0.net.node_id()).await;
    assert!(
        !rank0.net.document_sync_topic_exists(topic).unwrap_or(false),
        "a refused (held-but-not-a-member) probe must withhold creation, not fork a genesis"
    );

    // The co-holder's member top-up admits the rank-0 holder.
    process_shard_placements(&co_holder.context, realm_id, co_holder.net.node_id()).await;

    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        process_shard_placements(&rank0.context, realm_id, rank0.net.node_id()).await;
        if rank0.net.document_sync_topic_exists(topic).unwrap_or(false) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the rank-0 holder never adopted the existing genesis after the top-up"
        );
        sleep(Duration::from_millis(200)).await;
    }

    shutdown_nodes(nodes).await;
    Ok(())
}

fn rank0_shard_of(config: &RealmConfigDocument, rank0: NodeId, co_holder: NodeId) -> PlacementRef {
    for strategy in &config.strategies {
        for shard in 0..strategy.shard_count {
            let placement = PlacementRef {
                strategy_id: strategy.strategy_id,
                epoch: 0,
                shard,
            };
            let holders = resolve_shard_holders(config, &placement);
            if holders.first() == Some(&rank0) && holders.contains(&co_holder) {
                return placement;
            }
        }
    }
    panic!("no shard has {rank0} as rank-0 with {co_holder} as a co-holder");
}

fn seed_wrong_event_type_topic(
    net: &NetHandle,
    topic: ::irokle::TopicId,
) -> Result<(), Box<dyn std::error::Error>> {
    let node = net.document_sync_node();
    let actor_id = irokle::actor_id_for(topic, node.peer_id());
    let genesis = TopicGenesis {
        event_type_id: "aruna.test.wrong".to_string(),
        initial_peers: BTreeSet::new(),
        replication_policy: ReplicationPolicy::all(),
    };
    let oplog = Oplog::with_storage(node.storage().clone());
    oplog.create_topic_genesis(topic, actor_id, genesis, node.signer())?;
    Ok(())
}

async fn build_realm_nodes(
    realm_id: &RealmId,
    count: usize,
) -> Result<Vec<TestNode>, Box<dyn std::error::Error>> {
    let mut nodes = Vec::with_capacity(count);
    for _ in 0..count {
        nodes.push(spawn_node(*realm_id).await?);
    }
    Ok(nodes)
}

async fn mesh_nodes(nodes: &[TestNode]) {
    for i in 0..nodes.len() {
        for j in (i + 1)..nodes.len() {
            nodes[i]
                .net
                .add_peer_addr(nodes[j].net.endpoint_addr())
                .await;
            nodes[j]
                .net
                .add_peer_addr(nodes[i].net.endpoint_addr())
                .await;
        }
    }
}

async fn spawn_node(realm_id: RealmId) -> Result<TestNode, Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let storage = FjallStorage::open(temp_dir.path().to_str().ok_or("invalid temp path")?)?;
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
    .await?;
    let task_handle = TaskHandle::new();
    let context = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: Some(net.clone()),
        blob_handle: None,
        metadata_handle: None,
        task_handle: Some(task_handle.clone()),
    });
    initialize_net_incoming(context.clone());
    initialize_task_incoming(context.clone(), task_handle).await;
    Ok(TestNode {
        _temp_dir: temp_dir,
        net,
        context,
    })
}

async fn install_realm_config(
    nodes: &[TestNode],
    realm_id: RealmId,
) -> Result<RealmConfigDocument, Box<dyn std::error::Error>> {
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.seed_default_placement();
    for node in nodes {
        config.ensure_node(node.net.node_id(), RealmNodeKind::Management);
    }
    // Installs the config without running the placement reconciler, so each test
    // drives genesis creation explicitly.
    for node in nodes {
        let actor = Actor {
            node_id: node.net.node_id(),
            user_id: UserId::nil(realm_id),
            realm_id,
        };
        let bytes = config.to_bytes(&actor)?;
        match node
            .context
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Write {
                key_space: REALM_CONFIG_KEYSPACE.to_string(),
                key: (*realm_id.as_bytes()).into(),
                value: bytes.into(),
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => return Err(format!("unexpected realm config write event: {other:?}").into()),
        }
        node.net.refresh_realm_peers_from_document(&config).await?;
    }
    Ok(config)
}

async fn shutdown_nodes(nodes: Vec<TestNode>) {
    for node in nodes {
        node.net.shutdown().await;
    }
}
