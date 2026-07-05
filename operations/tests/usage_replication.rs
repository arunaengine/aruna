use std::sync::Arc;
use std::time::Duration;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{REALM_CONFIG_KEYSPACE, USAGE_STATS_KEYSPACE};
use aruna_core::structs::{
    Actor, BucketInfo, NODE_USAGE_DIRTY_GLOBAL_KEY, RealmConfigDocument, RealmId, RealmNodeKind,
    UsageCounters, node_usage_global_key, usage_global_key_for_group, usage_group_key,
};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::incoming::initialize_net_incoming;
use aruna_operations::s3::create_bucket::CreateBucketOperation;
use aruna_operations::task_incoming::initialize_task_incoming;
use aruna_operations::usage_stats::{RealmUsageScope, load_realm_usage, publish_usage_snapshots};
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use tempfile::TempDir;
use tokio::time::{Instant, sleep};
use ulid::Ulid;

const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(60);
// Comfortably above the durable re-arm interval (5s) plus the publish debounce
// (2s) so the steady-state publish is observed without flaking.
const STEADY_STATE_TIMEOUT: Duration = Duration::from_secs(30);

struct TestNode {
    _temp_dir: TempDir,
    net: NetHandle,
    context: Arc<DriverContext>,
}

// Node A's usage counters must become visible in node B's realm-wide aggregate
// via the real document sync apply path, while B's node-local counter keyspace
// stays completely untouched by ingest.
#[tokio::test]
async fn node_usage_snapshot_reaches_peer_realm_aggregate() -> Result<(), Box<dyn std::error::Error>>
{
    let realm_id = RealmId([37u8; 32]);
    let nodes = build_realm_nodes(&realm_id, 2).await?;
    let node_a = &nodes[0];
    let node_b = &nodes[1];
    let group_id = Ulid::new();

    drive(
        CreateBucketOperation::new(
            "usage-bucket".to_string(),
            BucketInfo {
                group_id,
                created_at: std::time::SystemTime::now(),
                created_by: Default::default(),
                cors_configuration: None,
            },
        ),
        node_a.context.as_ref(),
    )
    .await?
    .unwrap()
    .unwrap();

    publish_usage_snapshots(
        node_a.context.as_ref(),
        node_a.net.node_id(),
        realm_id,
        false,
    )
    .await?;

    // Node B receives A's snapshots over the sync layer and folds them into the
    // realm aggregate (global and per-group).
    let a_snapshot_key = node_usage_global_key(node_a.net.node_id());
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    loop {
        let received = read_node_stats(node_b, a_snapshot_key.clone())
            .await
            .is_some();
        let global = load_realm_usage(
            node_b.context.as_ref(),
            node_b.net.node_id(),
            RealmUsageScope::Global,
        )
        .await
        .unwrap_or_default();
        let group = load_realm_usage(
            node_b.context.as_ref(),
            node_b.net.node_id(),
            RealmUsageScope::Group(group_id),
        )
        .await
        .unwrap_or_default();
        if received && global.buckets == 1 && group.buckets == 1 {
            break;
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "node B never observed A's usage snapshot; received={received} global={global:?} group={group:?}"
            )
            .into());
        }
        sleep(Duration::from_millis(50)).await;
    }

    // Realm aggregate on B includes A's bucket even though B created nothing.
    let realm_global = load_realm_usage(
        node_b.context.as_ref(),
        node_b.net.node_id(),
        RealmUsageScope::Global,
    )
    .await?;
    assert_eq!(realm_global.buckets, 1, "realm global should include A");
    let realm_group = load_realm_usage(
        node_b.context.as_ref(),
        node_b.net.node_id(),
        RealmUsageScope::Group(group_id),
    )
    .await?;
    assert_eq!(realm_group.buckets, 1, "realm group should include A");

    // Ingest is counter-neutral: B's node-local usage keyspace is untouched.
    let local_stats = iter_usage_stats(node_b).await;
    assert!(
        local_stats.is_empty(),
        "replication ingest must not write B's node-local usage counters, found {} keys",
        local_stats.len()
    );

    shutdown_nodes(nodes).await;
    Ok(())
}

// A snapshot carrying every counter field (objects, blobs, stored/logical bytes)
// is the real ingest hazard: apply must fold it into node B's realm-wide
// aggregate (which lives in the node-usage keyspace) while leaving B's live
// incremental counter keyspace (`USAGE_STATS_KEYSPACE`) completely empty.
#[tokio::test]
async fn rich_node_usage_snapshot_ingest_is_counter_neutral()
-> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([41u8; 32]);
    let nodes = build_realm_nodes(&realm_id, 2).await?;
    let node_a = &nodes[0];
    let node_b = &nodes[1];
    let group_id = Ulid::new();

    // Seed node A's live counters with a full, non-trivial usage total, then let
    // the real publisher distribute it as node-usage snapshot documents.
    let rich = UsageCounters {
        buckets: 2,
        objects: 5,
        stored_blobs: 3,
        stored_bytes: 4096,
        logical_bytes: 8192,
    };
    write_usage_stat(node_a, usage_global_key_for_group(group_id), rich).await;
    write_usage_stat(node_a, usage_group_key(group_id), rich).await;

    publish_usage_snapshots(
        node_a.context.as_ref(),
        node_a.net.node_id(),
        realm_id,
        true,
    )
    .await?;

    let a_snapshot_key = node_usage_global_key(node_a.net.node_id());
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    loop {
        let received = read_node_stats(node_b, a_snapshot_key.clone())
            .await
            .is_some();
        let global = load_realm_usage(
            node_b.context.as_ref(),
            node_b.net.node_id(),
            RealmUsageScope::Global,
        )
        .await
        .unwrap_or_default();
        let group = load_realm_usage(
            node_b.context.as_ref(),
            node_b.net.node_id(),
            RealmUsageScope::Group(group_id),
        )
        .await
        .unwrap_or_default();
        if received && global.objects == rich.objects && group.objects == rich.objects {
            break;
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "node B never observed A's rich usage snapshot; received={received} global={global:?} group={group:?}"
            )
            .into());
        }
        sleep(Duration::from_millis(50)).await;
    }

    // Every field of A's snapshot surfaces in B's realm aggregate.
    let realm_global = load_realm_usage(
        node_b.context.as_ref(),
        node_b.net.node_id(),
        RealmUsageScope::Global,
    )
    .await?;
    assert_eq!(
        realm_global, rich,
        "realm global aggregate must equal A's snapshot"
    );
    let realm_group = load_realm_usage(
        node_b.context.as_ref(),
        node_b.net.node_id(),
        RealmUsageScope::Group(group_id),
    )
    .await?;
    assert_eq!(
        realm_group, rich,
        "realm group aggregate must equal A's snapshot"
    );

    // Ingest never wrote a single live incremental counter on B.
    let local_stats = iter_usage_stats(node_b).await;
    assert!(
        local_stats.is_empty(),
        "counter-bearing snapshot ingest must not touch B's live counter keyspace, found {} keys",
        local_stats.len()
    );

    shutdown_nodes(nodes).await;
    Ok(())
}

// A steady-state counter write on a long-running node (no restart) must lead to
// a published node-usage snapshot within a bounded window. The only thing that
// turns a durable dirty marker into a publish during steady state is the durable
// re-arm loop, so this proves that loop closes the gap the marker write leaves.
#[tokio::test]
async fn steady_state_write_publishes_snapshot_without_restart()
-> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([53u8; 32]);
    let nodes = build_realm_nodes(&realm_id, 1).await?;
    let node = &nodes[0];
    let group_id = Ulid::new();

    // A normal write commit updates counters and stamps dirty markers, but never
    // schedules a publish itself and never restarts the node.
    drive(
        CreateBucketOperation::new(
            "steady-bucket".to_string(),
            BucketInfo {
                group_id,
                created_at: std::time::SystemTime::now(),
                created_by: Default::default(),
                cors_configuration: None,
            },
        ),
        node.context.as_ref(),
    )
    .await?
    .unwrap()
    .unwrap();

    let snapshot_key = node_usage_global_key(node.net.node_id());
    let deadline = Instant::now() + STEADY_STATE_TIMEOUT;
    loop {
        let published = read_node_stats(node, snapshot_key.clone()).await.is_some();
        let dirty_cleared = read_node_stats(node, NODE_USAGE_DIRTY_GLOBAL_KEY.to_vec())
            .await
            .is_none();
        if published && dirty_cleared {
            break;
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "steady-state write never published a snapshot; published={published} dirty_cleared={dirty_cleared}"
            )
            .into());
        }
        sleep(Duration::from_millis(100)).await;
    }

    shutdown_nodes(nodes).await;
    Ok(())
}

async fn write_usage_stat(node: &TestNode, key: Vec<u8>, counters: UsageCounters) {
    match node
        .context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: USAGE_STATS_KEYSPACE.to_string(),
            key: key.into(),
            value: counters.to_bytes().unwrap().into(),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected usage stat write event: {other:?}"),
    }
}

async fn read_node_stats(node: &TestNode, key: Vec<u8>) -> Option<Vec<u8>> {
    match node
        .context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: aruna_core::keyspaces::USAGE_NODE_STATS_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value.map(|b| b.to_vec()),
        other => panic!("unexpected read event: {other:?}"),
    }
}

async fn iter_usage_stats(node: &TestNode) -> Vec<UsageCounters> {
    match node
        .context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Iter {
            key_space: USAGE_STATS_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: 4096,
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => values
            .into_iter()
            .map(|(_, value)| UsageCounters::from_bytes(value.as_ref()).unwrap())
            .collect(),
        other => panic!("unexpected iter event: {other:?}"),
    }
}

async fn build_realm_nodes(
    realm_id: &RealmId,
    count: usize,
) -> Result<Vec<TestNode>, Box<dyn std::error::Error>> {
    let mut nodes = Vec::with_capacity(count);
    for _ in 0..count {
        nodes.push(spawn_node(*realm_id).await?);
    }

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

    install_realm_config(&nodes, realm_id).await?;
    Ok(nodes)
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
    realm_id: &RealmId,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut config = RealmConfigDocument::default_for_realm(*realm_id, Vec::new());
    for node in nodes {
        config.ensure_node(node.net.node_id(), RealmNodeKind::Management);
    }

    for node in nodes {
        let actor = Actor {
            node_id: node.net.node_id(),
            user_id: aruna_core::UserId::nil(*realm_id),
            realm_id: *realm_id,
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

    Ok(())
}

async fn shutdown_nodes(nodes: Vec<TestNode>) {
    for node in nodes {
        node.net.shutdown().await;
    }
}
