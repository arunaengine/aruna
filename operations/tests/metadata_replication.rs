use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use aruna_core::structs::{Actor, MetadataDocument, RealmId};
use aruna_net::{NetConfig, NetHandle};
use aruna_operations::announce_realm_presence::{
    AnnounceRealmPresenceConfig, AnnounceRealmPresenceOperation,
};
use aruna_operations::automerge::AutomergeHandle;
use aruna_operations::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentOperation,
};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::ensure_realm_config::{EnsureRealmConfigConfig, EnsureRealmConfigOperation};
use aruna_operations::get_metadata_document::GetMetadataDocumentOperation;
use aruna_operations::get_realm_config::GetRealmConfigOperation;
use aruna_operations::get_realm_nodes::GetRealmNodesOperation;
use aruna_operations::incoming::initialize_net_incoming;
use aruna_operations::task_incoming::initialize_task_incoming;
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use tempfile::TempDir;
use tokio::time::{Instant, sleep};
use ulid::Ulid;

struct TestNode {
    _temp_dir: TempDir,
    net: NetHandle,
    context: Arc<DriverContext>,
}

#[tokio::test]
async fn metadata_creation_replicates_to_all_nodes_in_three_node_realm()
-> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([21u8; 32]);
    let nodes = build_realm_nodes(&realm_id, 3, 3).await?;
    let group_id = Ulid::new();
    let document_id = Ulid::new();

    let mut document = MetadataDocument::new(group_id, document_id, String::new());
    document
        .triples
        .insert("<http://example.org/root> <http://schema.org/name> \"three-node\"".to_string());

    let actor = Actor {
        node_id: nodes[0].net.node_id(),
        user_id: Ulid::new(),
        realm_id: realm_id.clone(),
    };

    drive(
        CreateMetadataDocumentOperation::new(CreateMetadataDocumentConfig {
            actor,
            document: document.clone(),
        }),
        nodes[0].context.as_ref(),
    )
    .await?;

    wait_for_metadata_replica_count(&nodes, group_id, document_id, &document, 3).await?;
    shutdown_nodes(nodes).await;
    Ok(())
}

#[tokio::test]
async fn metadata_creation_replicates_to_three_total_nodes_in_five_node_realm()
-> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([22u8; 32]);
    let nodes = build_realm_nodes(&realm_id, 5, 3).await?;
    let group_id = Ulid::new();
    let document_id = Ulid::new();

    let mut document = MetadataDocument::new(group_id, document_id, String::new());
    document
        .triples
        .insert("<http://example.org/root> <http://schema.org/name> \"five-node\"".to_string());

    let actor = Actor {
        node_id: nodes[0].net.node_id(),
        user_id: Ulid::new(),
        realm_id: realm_id.clone(),
    };

    drive(
        CreateMetadataDocumentOperation::new(CreateMetadataDocumentConfig {
            actor,
            document: document.clone(),
        }),
        nodes[0].context.as_ref(),
    )
    .await?;

    wait_for_metadata_replica_count(&nodes, group_id, document_id, &document, 3).await?;
    sleep(Duration::from_millis(300)).await;
    assert_eq!(
        metadata_replica_count(&nodes, group_id, document_id, &document).await,
        3
    );

    shutdown_nodes(nodes).await;
    Ok(())
}

async fn build_realm_nodes(
    realm_id: &RealmId,
    count: usize,
    replication_factor: usize,
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

    drive(
        EnsureRealmConfigOperation::new(EnsureRealmConfigConfig {
            actor: Actor {
                node_id: nodes[0].net.node_id(),
                user_id: Ulid::from_bytes([0u8; 16]),
                realm_id: realm_id.clone(),
            },
            bootstrap_peers: nodes
                .iter()
                .skip(1)
                .map(|node| node.net.node_id())
                .collect(),
            default_metadata_replication_factor: replication_factor as u32,
        }),
        nodes[0].context.as_ref(),
    )
    .await?;
    wait_for_realm_config_convergence(&nodes, realm_id, replication_factor).await?;

    for node in &nodes {
        drive(
            AnnounceRealmPresenceOperation::new(AnnounceRealmPresenceConfig {
                realm_id: realm_id.clone(),
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
            use_dns_discovery: false,
            ..NetConfig::default()
        },
        storage.clone(),
    )
    .await?;
    let task_handle = TaskHandle::new();
    let automerge_handle = AutomergeHandle::new(Some(net.clone()));

    let context = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: Some(net.clone()),
        blob_handle: None,
        automerge_handle: Some(automerge_handle),
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

async fn wait_for_realm_config_convergence(
    nodes: &[TestNode],
    realm_id: &RealmId,
    replication_factor: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(10);

    loop {
        let mut converged = true;
        for node in nodes {
            match drive(
                GetRealmConfigOperation::new(realm_id.clone()),
                node.context.as_ref(),
            )
            .await
            {
                Ok(document)
                    if document.metadata_replication.default_replication_factor
                        == replication_factor as u32 => {}
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
            return Err("realm config did not converge".into());
        }

        sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_realm_node_convergence(
    nodes: &[TestNode],
    realm_id: &RealmId,
) -> Result<(), Box<dyn std::error::Error>> {
    let expected: HashSet<_> = nodes.iter().map(|node| node.net.node_id()).collect();
    let deadline = Instant::now() + Duration::from_secs(20);

    loop {
        let mut converged = true;
        for node in nodes {
            match drive(
                GetRealmNodesOperation::new(realm_id.clone()),
                node.context.as_ref(),
            )
            .await
            {
                Ok(active_nodes) if active_nodes == expected => {}
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
            return Err("active realm nodes did not converge".into());
        }

        sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_metadata_replica_count(
    nodes: &[TestNode],
    group_id: Ulid,
    document_id: Ulid,
    expected_document: &MetadataDocument,
    expected_count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if metadata_replica_count(nodes, group_id, document_id, expected_document).await
            == expected_count
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "metadata document did not reach replica count {}",
                expected_count
            )
            .into());
        }
        sleep(Duration::from_millis(50)).await;
    }
}

async fn metadata_replica_count(
    nodes: &[TestNode],
    group_id: Ulid,
    document_id: Ulid,
    expected_document: &MetadataDocument,
) -> usize {
    let mut replicas = 0;
    for node in nodes {
        if let Ok(document) = drive(
            GetMetadataDocumentOperation::new(group_id, document_id),
            node.context.as_ref(),
        )
        .await
            && document == *expected_document
        {
            replicas += 1;
        }
    }
    replicas
}

async fn shutdown_nodes(nodes: Vec<TestNode>) {
    for node in nodes {
        node.net.shutdown().await;
    }
}
