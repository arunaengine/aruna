use std::sync::Arc;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::structs::{
    Actor, Group, GroupAuthorizationDocument, PlacementRef, RealmConfigDocument, RealmId,
    RealmNodeKind, shard_for_subject,
};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::create_group::{CreateGroupConfig, CreateGroupOperation};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::get_group::{GetGroupConfig, GetGroupOperation};
use aruna_operations::incoming::initialize_net_incoming;
use aruna_operations::placement::{
    PlacementResolutionContext, placement_ref_for_target, resolve_shard_holders,
};
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
    let (nodes, _config) = build_realm_nodes(&realm_id, 3).await?;

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

/// Five nodes at replication factor three, so a replica-capped bucket leaves real
/// non-holders — the three- and four-node fixtures cannot see this class of bug,
/// since there every node holds every bucket. The group is created on a node that
/// holds none of the group id's bucket under the realm's capped default strategy:
/// binding the group class to that strategy would leave the create unpublishable
/// (its shard topic cannot exist locally), the outbox record undeliverable, and
/// the group silently lost after an HTTP 200. Binding the class to `everywhere`
/// instead is what makes this converge — including the authorization document,
/// which `CheckPermissionsOperation` reads from the local `AUTH_KEYSPACE` and
/// hard-fails without.
#[tokio::test]
async fn unheld_group_replicates() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([33u8; 32]);
    let (nodes, config) = build_realm_nodes(&realm_id, 5).await?;
    let origin = nodes[0].net.node_id();

    let mut expected = None;
    for attempt in 0..32 {
        let created = drive(
            CreateGroupOperation::new(CreateGroupConfig {
                actor: Actor {
                    node_id: origin,
                    user_id: aruna_core::UserId::local(Ulid::r#gen(), realm_id),
                    realm_id,
                },
                display_name: format!("unheld group {attempt}"),
                owner_cap: None,
            }),
            nodes[0].context.as_ref(),
        )
        .await?;
        if !capped_holders(&config, created.0.group_id).contains(&origin) {
            expected = Some(created);
            break;
        }
    }
    let expected = expected.ok_or("no group id hashed outside the origin's capped buckets")?;

    wait_for_group_convergence(&nodes, expected.0.group_id, &expected.0, &expected.1).await?;

    // What makes the create publishable from an origin the capped strategy would
    // have excluded: every node holds the group's real bucket.
    let holders = group_holders(&config, expected.0.group_id);
    for node in &nodes {
        assert!(holders.contains(&node.net.node_id()));
    }

    shutdown_nodes(nodes).await;
    Ok(())
}

/// Holders of the bucket the group's authorization document hashes into, under
/// the strategy the realm actually binds the group class to.
fn group_holders(config: &RealmConfigDocument, group_id: Ulid) -> Vec<NodeId> {
    let target = DocumentSyncTarget::GroupAuthorization { group_id };
    let placement = placement_ref_for_target(
        config,
        &target,
        PlacementResolutionContext {
            group_id: Some(group_id),
            metadata_path: None,
        },
    );
    resolve_shard_holders(config, &placement)
}

/// Holders the group would have had under the realm's replica-capped default
/// strategy: the set the group class was wrongly bound to, and the one that
/// leaves non-holders at all.
fn capped_holders(config: &RealmConfigDocument, group_id: Ulid) -> Vec<NodeId> {
    let strategy = config
        .default_strategy_id
        .and_then(|id| config.strategy(&id))
        .expect("seeded default strategy");
    assert_eq!(strategy.replica_count, Some(3));
    resolve_shard_holders(
        config,
        &PlacementRef {
            strategy_id: strategy.strategy_id,
            epoch: 0,
            shard: shard_for_subject(&group_id.to_bytes(), strategy.shard_count),
        },
    )
}

async fn build_realm_nodes(
    realm_id: &RealmId,
    count: usize,
) -> Result<(Vec<TestNode>, RealmConfigDocument), Box<dyn std::error::Error>> {
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

    let config = install_realm_config(&nodes, realm_id).await?;
    Ok((nodes, config))
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
) -> Result<RealmConfigDocument, Box<dyn std::error::Error>> {
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
    // Config apply hook: the shard's rank-0 holder eagerly creates each shard
    // topic genesis and every other holder pulls it (mirrors the production
    // realm-config apply path). A holder whose rank-0 co-holder has not created
    // the genesis yet defers, so run the hook until nothing is left pending.
    for _ in 0..5 {
        for node in nodes {
            aruna_operations::startup::restore_shard_subscriptions(
                &node.context,
                node.net.node_id(),
                *realm_id,
            )
            .await;
        }
        let mut retry = false;
        for node in nodes {
            retry |= aruna_operations::process_placements::process_shard_placements(
                &node.context,
                *realm_id,
                node.net.node_id(),
            )
            .await
            .retry_scheduled;
        }
        if !retry {
            break;
        }
    }

    Ok(config)
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
