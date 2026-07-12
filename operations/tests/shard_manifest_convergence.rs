use std::sync::Arc;
use std::time::{Duration, Instant};

use aruna_core::document::{DocumentSyncTarget, ShardManifest, ShardManifestEntry};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::structs::{Actor, PlacementRef, RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::{NodeId, UserId};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::announce_realm_presence::{
    AnnounceRealmPresenceConfig, AnnounceRealmPresenceOperation,
};
use aruna_operations::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentOperation, CreateMetadataDocumentPayload,
};
use aruna_operations::delete_metadata_document::DeleteMetadataDocumentOperation;
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::get_realm_nodes::GetRealmNodesOperation;
use aruna_operations::incoming::initialize_net_incoming;
use aruna_operations::metadata::MetadataHandle;
use aruna_operations::placement::{choose_origin_bucket, strategy_for_target, subject_bytes};
use aruna_operations::shard::assemble_shard_manifest;
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

// Two holders write documents of one shard, interleaved from both sides. Once
// the shard drains and reconciles, both nodes must assemble byte-identical
// manifests: same entry set, same digest.
#[tokio::test]
async fn interleaved_writes_to_one_shard_converge_on_both_holders()
-> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([125u8; 32]);
    let (nodes, config) = build_realm_nodes(&realm_id, 2).await?;
    let group_id = Ulid::r#gen();

    // Pick one shard and gather several document ids both holders choose it for.
    let strategy = config.strategies.first().expect("a strategy").clone();
    let holders = [nodes[0].net.node_id(), nodes[1].net.node_id()];
    let target_shard = shard_of(&config, holders[0], Ulid::r#gen());
    let placement = PlacementRef {
        strategy_id: strategy.strategy_id,
        epoch: 0,
        shard: target_shard,
    };
    let document_ids = document_ids_in_shard(&config, &holders, target_shard, 6);

    // Interleave the creates across the two holders.
    for (index, document_id) in document_ids.iter().enumerate() {
        let node = &nodes[index % 2];
        create_document(node, realm_id, group_id, *document_id, index).await?;
    }

    // Both holders must converge to the same entry set and digest.
    wait_for_manifest_agreement(
        &nodes[0],
        &nodes[1],
        realm_id,
        placement,
        document_ids.len(),
    )
    .await?;

    let left = assemble_shard_manifest(nodes[0].context.as_ref(), realm_id, placement).await?;
    let right = assemble_shard_manifest(nodes[1].context.as_ref(), realm_id, placement).await?;
    assert_eq!(left.entries.len(), document_ids.len());
    assert_eq!(sorted_entries(&left), sorted_entries(&right));
    assert_eq!(left.digest, right.digest);

    // Delete one document and assert the tombstone converges byte-identically.
    // Receivers must stamp the ORIGIN's actor into the delete tombstone's
    // manifest row (not their own local node id), or the entry sets diverge
    // across holders even though the topic digests agree.
    let deleted_id = document_ids[0];
    let deleted_target = DocumentSyncTarget::MetadataDocumentLifecycle {
        document_id: deleted_id,
    };
    drive(
        DeleteMetadataDocumentOperation::new(
            Actor {
                node_id: nodes[0].net.node_id(),
                user_id: UserId::local(Ulid::r#gen(), realm_id),
                realm_id,
            },
            group_id,
            deleted_id,
        ),
        nodes[0].context.as_ref(),
    )
    .await?;

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let left = assemble_shard_manifest(nodes[0].context.as_ref(), realm_id, placement).await?;
        let right = assemble_shard_manifest(nodes[1].context.as_ref(), realm_id, placement).await?;
        let left_tomb = left
            .entries
            .iter()
            .find(|entry| entry.target == deleted_target)
            .map(|entry| entry.revision);
        let right_tomb = right
            .entries
            .iter()
            .find(|entry| entry.target == deleted_target)
            .map(|entry| entry.revision);
        if left_tomb.is_some() && left_tomb == right_tomb {
            // Both holders now carry the same tombstone row: the whole entry set
            // (tombstone included) and the digest must be identical.
            assert_eq!(
                left.entries.len(),
                document_ids.len(),
                "the delete keeps a tombstone row"
            );
            assert_eq!(sorted_entries(&left), sorted_entries(&right));
            assert_eq!(left.digest, right.digest);
            break;
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "delete tombstone did not converge across holders: left={left_tomb:?} right={right_tomb:?}"
            )
            .into());
        }
        sleep(Duration::from_millis(150)).await;
    }

    shutdown_nodes(nodes).await;
    Ok(())
}

fn shard_of(config: &RealmConfigDocument, origin: NodeId, document_id: Ulid) -> u32 {
    let target = DocumentSyncTarget::MetadataDocumentLifecycle { document_id };
    let (strategy, _) =
        strategy_for_target(config, &target, Default::default()).expect("strategy resolves");
    choose_origin_bucket(config, strategy, origin, &subject_bytes(&target))
        .expect("origin holds a bucket")
        .shard
}

fn document_ids_in_shard(
    config: &RealmConfigDocument,
    origins: &[NodeId],
    shard: u32,
    count: usize,
) -> Vec<Ulid> {
    let mut ids = Vec::with_capacity(count);
    while ids.len() < count {
        let candidate = Ulid::r#gen();
        if origins
            .iter()
            .all(|origin| shard_of(config, *origin, candidate) == shard)
        {
            ids.push(candidate);
        }
    }
    ids
}

fn sorted_entries(manifest: &ShardManifest) -> Vec<ShardManifestEntry> {
    let mut entries = manifest.entries.clone();
    entries.sort_by_key(|entry| postcard::to_allocvec(&entry.target).unwrap());
    entries
}

async fn create_document(
    node: &TestNode,
    realm_id: RealmId,
    group_id: Ulid,
    document_id: Ulid,
    index: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    drive(
        CreateMetadataDocumentOperation::new(CreateMetadataDocumentConfig {
            actor: Actor {
                node_id: node.net.node_id(),
                user_id: UserId::local(Ulid::r#gen(), realm_id),
                realm_id,
            },
            group_id,
            document_id,
            document_path: format!("datasets/converge-{index}"),
            public: true,
            payload: CreateMetadataDocumentPayload::Scaffold {
                name: format!("Converge {index}"),
                description: "convergence test".to_string(),
                date_published: "2026-07-07".to_string(),
                license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
            },
        }),
        node.context.as_ref(),
    )
    .await?;
    Ok(())
}

async fn wait_for_manifest_agreement(
    left: &TestNode,
    right: &TestNode,
    realm_id: RealmId,
    placement: PlacementRef,
    expected: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let left_manifest =
            assemble_shard_manifest(left.context.as_ref(), realm_id, placement).await?;
        let right_manifest =
            assemble_shard_manifest(right.context.as_ref(), realm_id, placement).await?;
        if left_manifest.entries.len() == expected
            && right_manifest.entries.len() == expected
            && left_manifest.digest == right_manifest.digest
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "manifests did not converge: left={} right={} digest_eq={}",
                left_manifest.entries.len(),
                right_manifest.entries.len(),
                left_manifest.digest == right_manifest.digest
            )
            .into());
        }
        sleep(Duration::from_millis(150)).await;
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
