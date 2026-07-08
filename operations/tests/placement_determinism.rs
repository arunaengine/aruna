use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use aruna_core::admin_document_reducer::AdminDocumentReducerState;
use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::document::{DocumentSyncPublish, DocumentSyncTarget};
use aruna_core::effects::{Effect, NetEffect, StorageEffect};
use aruna_core::events::{Event, NetEvent, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::structs::{Actor, NodePlacementEntry, RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::{DocumentSyncEffect, DocumentSyncNetEvent};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::get_realm_config::GetRealmConfigOperation;
use aruna_operations::incoming::initialize_net_incoming;
use aruna_operations::placement::{build_view, resolve_holders};
use aruna_operations::set_node_placement::{SetNodePlacementConfig, SetNodePlacementOperation};
use aruna_operations::task_incoming::initialize_task_incoming;
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use tempfile::TempDir;
use tokio::time::{Instant, sleep};

const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(60);

struct TestNode {
    _temp_dir: TempDir,
    net: NetHandle,
    context: Arc<DriverContext>,
}

// #261 Definition of Done: after a placement admin op propagates, any two nodes
// compute an identical holder set from the identical materialized record. Node A
// bumps a node's weight through the admin operation path; once B and C converge
// their RealmConfigDocument, all three must agree bit-for-bit and derive the
// same resolve_holders output for a stream of subjects.
#[tokio::test]
async fn weight_change_propagates_and_all_nodes_resolve_identically()
-> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([61u8; 32]);
    let nodes = build_realm_nodes(&realm_id, 3).await?;

    let bumped = nodes[1].net.node_id();
    let new_entry = NodePlacementEntry {
        node_id: bumped,
        location: "eu".to_string(),
        weight: 500,
        full: false,
        draining: false,
        labels: BTreeMap::new(),
    };

    let actor = Actor {
        node_id: nodes[0].net.node_id(),
        user_id: aruna_core::UserId::nil(realm_id),
        realm_id,
    };
    drive(
        SetNodePlacementOperation::new(SetNodePlacementConfig {
            actor,
            entry: new_entry.clone(),
        }),
        nodes[0].context.as_ref(),
    )
    .await?;

    // Wait until every node's materialized placement map reflects the new weight.
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    let mut last_states = Vec::new();
    loop {
        let mut converged = true;
        last_states.clear();
        for node in &nodes {
            let config = drive(
                GetRealmConfigOperation::new(realm_id),
                node.context.as_ref(),
            )
            .await?;
            let weight = config.placement_entry(bumped).map(|entry| entry.weight);
            last_states.push(format!("node={} weight={weight:?}", node.net.node_id()));
            if weight != Some(500) {
                converged = false;
                break;
            }
        }
        if converged {
            break;
        }
        if Instant::now() >= deadline {
            shutdown_nodes(nodes).await;
            return Err(format!(
                "weight change did not propagate to all nodes: {}",
                last_states.join(", ")
            )
            .into());
        }
        sleep(Duration::from_millis(100)).await;
    }

    // Identical records ⇒ identical placement state and identical holder sets.
    let mut configs = Vec::with_capacity(nodes.len());
    for node in &nodes {
        configs.push(
            drive(
                GetRealmConfigOperation::new(realm_id),
                node.context.as_ref(),
            )
            .await?,
        );
    }
    assert_placement_state_identical(&configs);
    assert_resolve_holders_identical(&configs);

    shutdown_nodes(nodes).await;
    Ok(())
}

fn assert_placement_state_identical(configs: &[RealmConfigDocument]) {
    let first = &configs[0];
    let mut first_map = first.placement_map.clone();
    first_map.sort_by(|a, b| a.node_id.as_bytes().cmp(b.node_id.as_bytes()));
    for config in &configs[1..] {
        let mut map = config.placement_map.clone();
        map.sort_by(|a, b| a.node_id.as_bytes().cmp(b.node_id.as_bytes()));
        assert_eq!(map, first_map, "placement_map diverged across nodes");
        assert_eq!(
            config.strategies, first.strategies,
            "strategies diverged across nodes"
        );
        assert_eq!(
            config.default_strategy_id, first.default_strategy_id,
            "default strategy diverged across nodes"
        );
        assert_eq!(
            config.strategy_bindings, first.strategy_bindings,
            "strategy bindings diverged across nodes"
        );
    }
}

fn assert_resolve_holders_identical(configs: &[RealmConfigDocument]) {
    let strategy = configs[0]
        .default_strategy_id
        .and_then(|id| configs[0].strategy(&id))
        .expect("realm config has a default strategy")
        .clone();

    let views: Vec<_> = configs.iter().map(build_view).collect();

    for counter in 0u64..100 {
        let subject = *blake3::hash(&counter.to_le_bytes()).as_bytes();
        let reference = resolve_holders(&views[0], &strategy, &subject, 0, None);
        assert!(
            !reference.is_empty(),
            "empty holder set for subject {counter}"
        );
        for view in &views[1..] {
            let holders = resolve_holders(view, &strategy, &subject, 0, None);
            assert_eq!(
                holders, reference,
                "holder set diverged for subject {counter}"
            );
        }
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
    mesh_nodes(&nodes).await;
    install_realm_config(&nodes, *realm_id).await?;
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

// Base config: three nodes across two locations with mixed weights, plus a
// distinct-location default strategy so the resolver walks both levels.
fn base_config(nodes: &[TestNode], realm_id: RealmId) -> RealmConfigDocument {
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.seed_default_placement();
    let default_id = config.default_strategy_id.expect("seeded default strategy");
    if let Some(strategy) = config
        .strategies
        .iter_mut()
        .find(|strategy| strategy.strategy_id == default_id)
    {
        strategy.distinct_locations = true;
    }

    let locations = ["eu", "eu", "us"];
    let weights = [100u32, 200, 150];
    for (index, node) in nodes.iter().enumerate() {
        let node_id = node.net.node_id();
        config.ensure_node(node_id, RealmNodeKind::Management);
        config.placement_map.push(NodePlacementEntry {
            node_id,
            location: locations[index % locations.len()].to_string(),
            weight: weights[index % weights.len()],
            full: false,
            draining: false,
            labels: BTreeMap::new(),
        });
    }
    config
}

async fn install_realm_config(
    nodes: &[TestNode],
    realm_id: RealmId,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = base_config(nodes, realm_id);
    for node in nodes {
        let actor = Actor {
            node_id: node.net.node_id(),
            user_id: aruna_core::UserId::nil(realm_id),
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
    seed_realm_config_sync_topic(nodes, realm_id, &config).await?;
    // Config apply hook: the shard's rank-0 holder eagerly creates each
    // shard topic genesis (mirrors the production realm-config apply path).
    for node in nodes {
        aruna_operations::process_placements::process_shard_placements(
            &node.context,
            realm_id,
            node.net.node_id(),
        )
        .await;
    }
    Ok(())
}

async fn seed_realm_config_sync_topic(
    nodes: &[TestNode],
    realm_id: RealmId,
    config: &RealmConfigDocument,
) -> Result<(), Box<dyn std::error::Error>> {
    let target = DocumentSyncTarget::RealmConfig { realm_id };
    let placement = aruna_operations::placement::placement_ref_for_target(config, &target, None);
    let topic = target.sync_topic_id(realm_id, &placement);
    let actor = Actor {
        node_id: nodes[1].net.node_id(),
        user_id: aruna_core::UserId::nil(realm_id),
        realm_id,
    };
    let mut reducer_state =
        AdminDocumentReducerState::new(AdminDocumentTarget::RealmConfig { realm_id });
    let event = reducer_state.apply_operation(
        &actor,
        AdminDocumentOperation::RealmConfigNodePlacementSet {
            entry: config
                .placement_map
                .first()
                .ok_or("realm config has no placement entry")?
                .clone(),
        },
    )?;

    match nodes[0]
        .net
        .send_effect(Effect::Net(NetEffect::DocumentSync(
            DocumentSyncEffect::PublishDocuments {
                documents: vec![DocumentSyncPublish::AdminOperation {
                    target: target.clone(),
                    event: Box::new(event),
                    placement,
                    allow_genesis: true,
                }],
                peers: Vec::new(),
            },
        )))
        .await
    {
        Event::Net(NetEvent::DocumentSync(DocumentSyncNetEvent::DocumentsPublished { .. })) => {}
        other => {
            return Err(format!("unexpected realm config seed publish event: {other:?}").into());
        }
    }

    match nodes[0]
        .net
        .send_effect(Effect::Net(NetEffect::DocumentSync(
            DocumentSyncEffect::SyncDocuments {
                topics: vec![topic],
                peers: Vec::new(),
            },
        )))
        .await
    {
        Event::Net(NetEvent::DocumentSync(DocumentSyncNetEvent::DocumentsReconciled {
            ..
        })) => Ok(()),
        other => Err(format!("unexpected realm config seed sync event: {other:?}").into()),
    }
}

async fn shutdown_nodes(nodes: Vec<TestNode>) {
    for node in nodes {
        node.net.shutdown().await;
    }
}
