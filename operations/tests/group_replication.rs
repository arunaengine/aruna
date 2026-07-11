use std::sync::Arc;
use std::time::Duration;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::structs::{
    Actor, Group, GroupAuthorizationDocument, RealmConfigDocument, RealmId, RealmNodeKind,
};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::create_group::{CreateGroupConfig, CreateGroupOperation};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::get_group::{GetGroupConfig, GetGroupOperation};
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
        user_id: aruna_core::UserId::local(Ulid::r#gen(), realm_id),
        realm_id,
    };

    let expected = drive(
        CreateGroupOperation::new(CreateGroupConfig {
            actor: creator,
            display_name: "replicated group".to_string(),
            owner_cap: None,
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
    config.seed_default_placement();
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
