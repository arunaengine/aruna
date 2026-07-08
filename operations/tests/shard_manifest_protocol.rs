use std::sync::Arc;
use std::time::{Duration, Instant};

use aruna_core::document::{DocumentSyncTarget, ShardManifest};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::structs::{
    Actor, PlacementRef, PlacementStrategy, RealmConfigDocument, RealmId, RealmNodeKind,
};
use aruna_core::{NodeId, UserId};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::announce_realm_presence::{
    AnnounceRealmPresenceConfig, AnnounceRealmPresenceOperation,
};
use aruna_operations::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentOperation, CreateMetadataDocumentPayload,
};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::get_realm_nodes::GetRealmNodesOperation;
use aruna_operations::incoming::initialize_net_incoming;
use aruna_operations::metadata::MetadataHandle;
use aruna_operations::placement::placement_ref_for_target;
use aruna_operations::shard::assemble_shard_manifest;
use aruna_operations::shard::client::fetch_shard_manifest;
use aruna_operations::shard::verify::{is_shard_verified, verify_held_shards};
use aruna_operations::task_incoming::initialize_task_incoming;
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use tempfile::TempDir;
use tokio::time::sleep;
use ulid::Ulid;

struct TestNode {
    _temp_dir: TempDir,
    net: NetHandle,
    context: Arc<DriverContext>,
}

#[tokio::test]
async fn manifest_request_rejected_from_non_realm_peer() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([121u8; 32]);
    let (nodes, config) = build_realm_nodes(&realm_id, 2).await?;
    // An outsider node that is not in the realm config at all.
    let outsider = spawn_node(realm_id).await?;
    outsider
        .net
        .add_peer_addr(nodes[0].net.endpoint_addr())
        .await;
    nodes[0]
        .net
        .add_peer_addr(outsider.net.endpoint_addr())
        .await;

    let placement = any_held_placement(&config, nodes[0].net.node_id());
    let error = fetch_shard_manifest(&outsider.net, nodes[0].net.node_id(), realm_id, placement)
        .await
        .expect_err("non-realm peer must be rejected");
    assert!(
        error.contains("not a sync-eligible node"),
        "unexpected reject: {error}"
    );

    outsider.net.shutdown().await;
    shutdown_nodes(nodes).await;
    Ok(())
}

#[tokio::test]
async fn manifest_request_rejected_for_non_held_shard() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([123u8; 32]);
    let (nodes, _config) = build_realm_nodes(&realm_id, 2).await?;

    // A placement whose strategy the config does not know resolves to no holders,
    // so the responder never holds it.
    let bogus = PlacementRef {
        strategy_id: Ulid::from_bytes([0xAB; 16]),
        epoch: 0,
        shard: 1,
    };
    let error = fetch_shard_manifest(&nodes[1].net, nodes[0].net.node_id(), realm_id, bogus)
        .await
        .expect_err("non-held shard must be rejected");
    assert!(
        error.contains("does not hold shard"),
        "unexpected reject: {error}"
    );

    shutdown_nodes(nodes).await;
    Ok(())
}

#[tokio::test]
async fn manifest_request_rejected_from_sync_eligible_non_holder()
-> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([127u8; 32]);
    let (nodes, config) = build_realm_nodes(&realm_id, 4).await?;
    let responder = nodes[0].net.node_id();
    let (placement, non_holder, holder) = placement_with_non_holder_requester(&config, &nodes);

    let non_holder_node = nodes
        .iter()
        .find(|node| node.net.node_id() == non_holder)
        .expect("non-holder node exists");
    let error = fetch_shard_manifest(&non_holder_node.net, responder, realm_id, placement)
        .await
        .expect_err("sync-eligible non-holder peer must be rejected");
    assert!(
        error.contains("is not a holder"),
        "unexpected reject: {error}"
    );

    let holder_node = nodes
        .iter()
        .find(|node| node.net.node_id() == holder)
        .expect("holder node exists");
    let manifest = fetch_shard_manifest(&holder_node.net, responder, realm_id, placement).await?;
    assert_eq!(manifest.holder, responder);
    assert_eq!(manifest.placement, placement);

    shutdown_nodes(nodes).await;
    Ok(())
}

#[tokio::test]
async fn new_holder_verifies_shard_against_co_holder() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([124u8; 32]);
    let (nodes, config) = build_realm_nodes(&realm_id, 2).await?;
    let group_id = Ulid::new();

    let created = drive(
        CreateMetadataDocumentOperation::new(CreateMetadataDocumentConfig {
            actor: Actor {
                node_id: nodes[0].net.node_id(),
                user_id: UserId::local(Ulid::new(), realm_id),
                realm_id,
            },
            group_id,
            document_id: Ulid::new(),
            document_path: "datasets/verify-canary".to_string(),
            public: true,
            payload: CreateMetadataDocumentPayload::Scaffold {
                name: "Verify Canary".to_string(),
                description: "verification test".to_string(),
                date_published: "2026-07-07".to_string(),
                license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
            },
        }),
        nodes[0].context.as_ref(),
    )
    .await?;
    let target = DocumentSyncTarget::MetadataDocumentLifecycle {
        document_id: created.record.document_id,
    };
    let placement = placement_ref_for_target(&config, &target, None);

    // Wait until node B (the co-holder) has synced the document into its shard.
    wait_for_manifest_entry(&nodes[1], realm_id, placement, &target).await?;

    // Before verification, node B has no marker for that shard.
    assert!(!is_shard_verified(nodes[1].context.as_ref(), realm_id, &placement).await);

    // Node B fetches node A's manifest directly and sees the same document.
    let remote =
        fetch_shard_manifest(&nodes[1].net, nodes[0].net.node_id(), realm_id, placement).await?;
    assert!(remote.entries.iter().any(|entry| entry.target == target));

    let summary = verify_held_shards(&nodes[1].context, nodes[1].net.node_id(), realm_id).await;
    assert!(
        summary.newly_verified > 0,
        "expected at least one shard verified, got {summary:?}"
    );
    assert!(
        is_shard_verified(nodes[1].context.as_ref(), realm_id, &placement).await,
        "the document's shard must be verified"
    );

    // A second pass finds every held shard already verified (idempotent, cheap).
    let second = verify_held_shards(&nodes[1].context, nodes[1].net.node_id(), realm_id).await;
    assert_eq!(second.newly_verified, 0);
    assert_eq!(second.already_verified, second.held);

    shutdown_nodes(nodes).await;
    Ok(())
}

// Two genesis-less holders compute the SAME (non-zero) empty fingerprint, so
// their digests match — exactly the condition that would falsely certify
// convergence. Verification must still refuse to mark the shard verified,
// because neither has a local genesis.
#[tokio::test]
async fn co_holders_with_no_genesis_do_not_verify_on_matching_empty_digest()
-> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([126u8; 32]);
    let nodes = build_meshed_nodes(realm_id, 2).await?;
    let config = install_config_without_placements(&nodes, realm_id, 2).await?;

    let placement = any_held_placement(&config, nodes[1].net.node_id());
    let left = assemble_shard_manifest(nodes[0].context.as_ref(), realm_id, placement).await?;
    let right = assemble_shard_manifest(nodes[1].context.as_ref(), realm_id, placement).await?;
    assert_eq!(
        left.digest, right.digest,
        "genesis-less holders share the empty fingerprint"
    );
    assert!(
        !nodes[1]
            .net
            .document_sync_topic_exists(aruna_core::document::shard_topic_id(realm_id, &placement))
            .unwrap_or(false),
        "no local genesis exists for the shard"
    );

    let summary = verify_held_shards(&nodes[1].context, nodes[1].net.node_id(), realm_id).await;
    assert_eq!(
        summary.newly_verified, 0,
        "a matching empty digest must not verify a genesis-less shard: {summary:?}"
    );
    assert!(
        !is_shard_verified(nodes[1].context.as_ref(), realm_id, &placement).await,
        "a genesis-less shard must stay unverified"
    );

    shutdown_nodes(nodes).await;
    Ok(())
}

async fn build_meshed_nodes(
    realm_id: RealmId,
    count: usize,
) -> Result<Vec<TestNode>, Box<dyn std::error::Error>> {
    let mut nodes = Vec::with_capacity(count);
    for _ in 0..count {
        nodes.push(spawn_node(realm_id).await?);
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
    Ok(nodes)
}

// Installs a small-shard realm config (every node holds every shard) but does
// not run the placement reconciler, so no shard topic genesis exists.
async fn install_config_without_placements(
    nodes: &[TestNode],
    realm_id: RealmId,
    shard_count: u32,
) -> Result<RealmConfigDocument, Box<dyn std::error::Error>> {
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    let strategy = PlacementStrategy {
        strategy_id: Ulid::new(),
        name: "default".to_string(),
        replica_count: None,
        distinct_locations: false,
        affinity: Vec::new(),
        shard_count,
    };
    config.default_strategy_id = Some(strategy.strategy_id);
    config.strategies = vec![strategy];
    for node in nodes {
        config.ensure_node(node.net.node_id(), RealmNodeKind::Management);
    }
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

fn any_held_placement(config: &RealmConfigDocument, node_id: NodeId) -> PlacementRef {
    let strategy = config.strategies.first().expect("a strategy");
    for shard in 0..strategy.shard_count {
        let placement = PlacementRef {
            strategy_id: strategy.strategy_id,
            epoch: 0,
            shard,
        };
        if aruna_operations::placement::resolve_shard_holders(config, &placement).contains(&node_id)
        {
            return placement;
        }
    }
    panic!("node holds no shard");
}

fn placement_with_non_holder_requester(
    config: &RealmConfigDocument,
    nodes: &[TestNode],
) -> (PlacementRef, NodeId, NodeId) {
    let responder = nodes[0].net.node_id();
    let strategy = config.strategies.first().expect("a strategy");
    for shard in 0..strategy.shard_count {
        let placement = PlacementRef {
            strategy_id: strategy.strategy_id,
            epoch: 0,
            shard,
        };
        let holders = aruna_operations::placement::resolve_shard_holders(config, &placement);
        if !holders.contains(&responder) {
            continue;
        }
        let Some(non_holder) = nodes
            .iter()
            .map(|node| node.net.node_id())
            .find(|node_id| *node_id != responder && !holders.contains(node_id))
        else {
            continue;
        };
        let holder = holders
            .iter()
            .copied()
            .find(|node_id| *node_id != responder)
            .expect("responder has a co-holder");
        return (placement, non_holder, holder);
    }
    panic!("no shard found with a holder responder and sync-eligible non-holder requester");
}

async fn wait_for_manifest_entry(
    node: &TestNode,
    realm_id: RealmId,
    placement: PlacementRef,
    target: &DocumentSyncTarget,
) -> Result<ShardManifest, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let manifest = assemble_shard_manifest(node.context.as_ref(), realm_id, placement).await?;
        if manifest.entries.iter().any(|entry| &entry.target == target) {
            return Ok(manifest);
        }
        if Instant::now() >= deadline {
            return Err("timed out waiting for manifest entry".into());
        }
        sleep(Duration::from_millis(100)).await;
    }
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
    let metadata_handle = MetadataHandle::new(
        temp_dir.path().join("metadata"),
        net.node_id(),
        storage.clone(),
        Some(net.clone()),
        Some(net.document_sync_node()),
        Some(net.document_sync_database()),
    )?;
    let context = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: Some(net.clone()),
        blob_handle: None,
        metadata_handle: Some(metadata_handle),
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
            user_id: UserId::nil(*realm_id),
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
    for node in nodes {
        aruna_operations::process_placements::process_shard_placements(
            &node.context,
            *realm_id,
            node.net.node_id(),
        )
        .await;
    }
    Ok(config)
}

async fn wait_for_realm_node_convergence(
    nodes: &[TestNode],
    realm_id: &RealmId,
) -> Result<(), Box<dyn std::error::Error>> {
    let expected: std::collections::HashSet<NodeId> =
        nodes.iter().map(|node| node.net.node_id()).collect();
    let deadline = Instant::now() + Duration::from_secs(30);
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

async fn shutdown_nodes(nodes: Vec<TestNode>) {
    for node in nodes {
        node.net.shutdown().await;
    }
}
