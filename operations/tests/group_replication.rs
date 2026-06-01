use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use aruna_core::structs::{Actor, Group, GroupAuthorizationDocument, RealmId};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::announce_realm_presence::{
    AnnounceRealmPresenceConfig, AnnounceRealmPresenceOperation,
};
use aruna_operations::create_group::{CreateGroupConfig, CreateGroupOperation};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::get_group::{GetGroupConfig, GetGroupOperation};
use aruna_operations::get_realm_nodes::GetRealmNodesOperation;
use aruna_operations::incoming::initialize_net_incoming;
use aruna_operations::task_incoming::initialize_task_incoming;
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use tempfile::TempDir;
use tokio::time::{Instant, sleep};
use ulid::Ulid;

const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(60);

struct TestNode {
    _temp_dir: TempDir,
    net: NetHandle,
    context: Arc<DriverContext>,
}

#[tokio::test]
async fn group_creation_replicates_to_all_realm_nodes() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([31u8; 32]);
    let nodes = build_realm_nodes(&realm_id, 3).await?;

    let creator = Actor {
        node_id: nodes[0].net.node_id(),
        user_id: aruna_core::UserId::local(Ulid::new(), realm_id),
        realm_id,
    };

    let expected = drive(
        CreateGroupOperation::new(CreateGroupConfig {
            actor: creator,
            display_name: "replicated group".to_string(),
        }),
        nodes[0].context.as_ref(),
    )
    .await?;

    wait_for_group_convergence(&nodes, expected.0.group_id, &expected.0, &expected.1).await?;
    shutdown_nodes(nodes).await;
    Ok(())
}

async fn build_realm_nodes(
    realm_id: &RealmId,
    count: usize,
) -> Result<Vec<TestNode>, Box<dyn std::error::Error>> {
    let mut nodes = Vec::with_capacity(count);
    for _ in 0..count {
        nodes.push(spawn_node().await?);
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

    for node in &nodes {
        drive(
            AnnounceRealmPresenceOperation::new(AnnounceRealmPresenceConfig {
                realm_id: *realm_id,
                node_id: node.net.node_id(),
                schedule_refresh: true,
            }),
            node.context.as_ref(),
        )
        .await?;
    }

    wait_for_realm_node_convergence(&nodes, realm_id).await?;
    Ok(nodes)
}

async fn spawn_node() -> Result<TestNode, Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let storage = FjallStorage::open(temp_dir.path().to_str().ok_or("invalid temp path")?)?;
    let net = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().expect("valid bind addr"),
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

async fn wait_for_realm_node_convergence(
    nodes: &[TestNode],
    realm_id: &RealmId,
) -> Result<(), Box<dyn std::error::Error>> {
    let expected: HashSet<_> = nodes.iter().map(|node| node.net.node_id()).collect();
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;

    loop {
        let mut converged = true;
        for node in nodes {
            match drive(
                GetRealmNodesOperation::new(*realm_id),
                node.context.as_ref(),
            )
            .await
            {
                Ok(realm_nodes) if realm_nodes == expected => {}
                _ => {
                    converged = false;
                    break;
                }
            }
        }

        if converged {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("realm nodes did not converge".into());
        }

        sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_group_convergence(
    nodes: &[TestNode],
    group_id: Ulid,
    expected_group: &Group,
    expected_auth: &GroupAuthorizationDocument,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    let mut last_states = Vec::new();

    loop {
        let mut converged = true;
        last_states.clear();
        for node in nodes {
            match drive(
                GetGroupOperation::new(GetGroupConfig { group_id }),
                node.context.as_ref(),
            )
            .await
            {
                Ok((group, auth)) if group == *expected_group && auth == *expected_auth => {
                    last_states.push(format!("node={} converged", node.net.node_id()));
                }
                Ok((group, auth)) => {
                    last_states.push(format!(
                        "node={} group={group:?} auth={auth:?}",
                        node.net.node_id()
                    ));
                    converged = false;
                    break;
                }
                Err(error) => {
                    last_states.push(format!("node={} error={error:?}", node.net.node_id()));
                    converged = false;
                    break;
                }
            }
        }

        if converged {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "group state did not converge; expected_group={expected_group:?}; expected_auth={expected_auth:?}; last_states={last_states:?}"
            )
            .into());
        }

        sleep(Duration::from_millis(50)).await;
    }
}

async fn shutdown_nodes(nodes: Vec<TestNode>) {
    for node in nodes {
        node.net.shutdown().await;
    }
}
