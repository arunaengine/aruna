use std::sync::Arc;
use std::time::{Duration, Instant};

use aruna_core::document::{DocumentSyncTarget, ShardManifest};
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
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::get_realm_nodes::GetRealmNodesOperation;
use aruna_operations::incoming::initialize_net_incoming;
use aruna_operations::metadata::MetadataHandle;
use aruna_operations::placement::placement_ref_for_target;
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

// A metadata document created on node A must leave a manifest row on the origin
// AND on the receiver that syncs it, and both holders' assembled shard manifests
// must agree on the entry set and the topic digest.
#[tokio::test]
async fn document_manifest_row_lands_on_origin_and_receiver_with_matching_digest()
-> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([120u8; 32]);
    let (nodes, config) = build_realm_nodes(&realm_id, 2).await?;
    let group_id = Ulid::r#gen();
    let document_id = Ulid::r#gen();

    let created = drive(
        CreateMetadataDocumentOperation::new(CreateMetadataDocumentConfig {
            actor: Actor {
                node_id: nodes[0].net.node_id(),
                user_id: UserId::local(Ulid::r#gen(), realm_id),
                realm_id,
            },
            group_id,
            document_id,
            document_path: "datasets/manifest-canary".to_string(),
            public: true,
            payload: CreateMetadataDocumentPayload::Scaffold {
                name: "Manifest Canary".to_string(),
                description: "manifest maintenance test".to_string(),
                date_published: "2026-07-07".to_string(),
                license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
            },
        }),
        nodes[0].context.as_ref(),
    )
    .await?;
    let document_id = created.record.document_id;

    let target = DocumentSyncTarget::MetadataDocumentLifecycle { document_id };
    let placement = placement_ref_for_target(&config, &target, Default::default());
    assert_ne!(
        placement,
        PlacementRef::NIL,
        "document must resolve a shard"
    );

    // The receiver only writes its manifest row once the lifecycle syncs and
    // applies, so poll node B until the document appears.
    let receiver = wait_for_manifest_entry(&nodes[1], realm_id, placement, &target).await?;
    let origin = assemble_shard_manifest(nodes[0].context.as_ref(), realm_id, placement).await?;

    assert!(
        manifest_contains(&origin, &target),
        "origin manifest missing the document"
    );
    assert!(
        manifest_contains(&receiver, &target),
        "receiver manifest missing the document"
    );
    // Same shard, converged topic ⇒ identical digest across holders.
    assert_eq!(
        origin.digest, receiver.digest,
        "co-holder shard digests must agree"
    );
    // The lifecycle revision the origin recorded is the one the receiver applied.
    let origin_revision = manifest_revision(&origin, &target);
    let receiver_revision = manifest_revision(&receiver, &target);
    assert_eq!(origin_revision, receiver_revision);

    shutdown_nodes(nodes).await;
    Ok(())
}

fn manifest_contains(manifest: &ShardManifest, target: &DocumentSyncTarget) -> bool {
    manifest.entries.iter().any(|entry| &entry.target == target)
}

fn manifest_revision(
    manifest: &ShardManifest,
    target: &DocumentSyncTarget,
) -> aruna_core::document::DocumentSyncRevision {
    manifest
        .entries
        .iter()
        .find(|entry| &entry.target == target)
        .map(|entry| entry.revision)
        .expect("manifest entry present")
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
        if manifest_contains(&manifest, target) {
            return Ok(manifest);
        }
        if Instant::now() >= deadline {
            return Err("timed out waiting for receiver manifest entry".into());
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
