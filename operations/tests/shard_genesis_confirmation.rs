use std::sync::Arc;
use std::time::{Duration, Instant};

use aruna_core::document::shard_topic_id;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::structs::{Actor, PlacementRef, RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::{NodeId, UserId};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::driver::DriverContext;
use aruna_operations::incoming::initialize_net_incoming;
use aruna_operations::placement::resolve_shard_holders;
use aruna_operations::process_placements::process_shard_placements;
use aruna_operations::task_incoming::initialize_task_incoming;
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use tempfile::TempDir;
use tokio::time::sleep;

struct TestNode {
    _temp_dir: TempDir,
    net: NetHandle,
    context: Arc<DriverContext>,
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

// An unreachable co-holder might hold a genesis, so creation is withheld; once
// the co-holder is reachable again, the retry creates it.
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

    process_shard_placements(&rank0.context, realm_id, rank0.net.node_id()).await;
    assert!(
        !rank0.net.document_sync_topic_exists(topic).unwrap_or(false),
        "an unreachable co-holder must withhold genesis creation (a fork would be permanent)"
    );

    // The co-holder returns.
    mesh_nodes(&nodes).await;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        process_shard_placements(&rank0.context, realm_id, rank0.net.node_id()).await;
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
