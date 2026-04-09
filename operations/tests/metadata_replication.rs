use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::handle::Handle;
use aruna_core::metadata::{MetadataEffect, MetadataEvent};
use aruna_core::structs::{Actor, RealmId};
use aruna_net::{NetConfig, NetHandle};
use aruna_operations::announce_realm_presence::{
    AnnounceRealmPresenceConfig, AnnounceRealmPresenceOperation,
};
use aruna_operations::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentOperation,
};
use aruna_operations::delete_metadata_document::DeleteMetadataDocumentOperation;
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::get_metadata_document::GetMetadataDocumentOperation;
use aruna_operations::get_realm_nodes::GetRealmNodesOperation;
use aruna_operations::incoming::initialize_net_incoming;
use aruna_operations::metadata::MetadataHandle;
use aruna_operations::task_incoming::initialize_task_incoming;
use aruna_operations::update_metadata_document::{
    UpdateMetadataDocumentConfig, UpdateMetadataDocumentOperation,
};
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
async fn metadata_creation_bootstraps_selected_holders() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([41u8; 32]);
    let nodes = build_realm_nodes(&realm_id, 3).await?;
    let group_id = Ulid::new();
    let document_id = Ulid::new();

    let visible_nodes = drive(
        GetRealmNodesOperation::new(realm_id.clone()),
        nodes[0].context.as_ref(),
    )
    .await?;
    assert_eq!(visible_nodes.len(), 3);

    let created = drive(
        CreateMetadataDocumentOperation::new(CreateMetadataDocumentConfig {
            actor: Actor {
                node_id: nodes[0].net.node_id(),
                user_id: Ulid::new(),
                realm_id: realm_id.clone(),
            },
            group_id,
            document_id,
            document_path: "datasets/bootstrap".to_string(),
            name: "Bootstrap Dataset".to_string(),
            description: "Replicated metadata".to_string(),
            date_published: "2026-01-01".to_string(),
            license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
            public: true,
        }),
        nodes[0].context.as_ref(),
    )
    .await?;

    let expected_holders: HashSet<_> = nodes.iter().map(|node| node.net.node_id()).collect();
    assert_eq!(
        created
            .holder_node_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>(),
        expected_holders
    );

    wait_for_metadata_convergence(&nodes, group_id, document_id, &created.graph_iri).await?;
    shutdown_nodes(nodes).await;
    Ok(())
}

#[tokio::test]
async fn metadata_updates_and_deletes_replicate_to_holders()
-> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([42u8; 32]);
    let nodes = build_realm_nodes(&realm_id, 3).await?;
    let group_id = Ulid::new();
    let document_id = Ulid::new();

    let created = drive(
        CreateMetadataDocumentOperation::new(CreateMetadataDocumentConfig {
            actor: Actor {
                node_id: nodes[0].net.node_id(),
                user_id: Ulid::new(),
                realm_id: realm_id.clone(),
            },
            group_id,
            document_id,
            document_path: "datasets/propagation".to_string(),
            name: "Initial Dataset".to_string(),
            description: "Initial description".to_string(),
            date_published: "2026-01-01".to_string(),
            license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
            public: false,
        }),
        nodes[0].context.as_ref(),
    )
    .await?;

    wait_for_metadata_state(
        &nodes,
        group_id,
        document_id,
        &created.graph_iri,
        nodes.len(),
        "Initial Dataset",
    )
    .await?;

    let updated_jsonld = format!(
        r#"{{
  "@context": "https://w3id.org/ro/crate/1.2/context",
  "@graph": [
    {{
      "@id": "ro-crate-metadata.json",
      "@type": "CreativeWork",
      "conformsTo": {{"@id": "https://w3id.org/ro/crate/1.2"}},
      "about": {{"@id": "https://w3id.org/aruna/{document_id}"}}
    }},
    {{
      "@id": "https://w3id.org/aruna/{document_id}",
      "@type": "Dataset",
      "name": "Updated Dataset",
      "description": "Replicated update",
      "datePublished": "2026-02-01",
      "license": {{"@id": "https://creativecommons.org/licenses/by/4.0/"}}
    }}
  ]
}}"#
    );

    let updated = drive(
        UpdateMetadataDocumentOperation::new(UpdateMetadataDocumentConfig {
            actor: Actor {
                node_id: nodes[0].net.node_id(),
                user_id: Ulid::new(),
                realm_id: realm_id.clone(),
            },
            group_id,
            document_id,
            jsonld: updated_jsonld,
            public: true,
        }),
        nodes[0].context.as_ref(),
    )
    .await?;
    assert!(updated.public);

    wait_for_metadata_state(
        &nodes[1..],
        group_id,
        document_id,
        &created.graph_iri,
        nodes.len(),
        "Updated Dataset",
    )
    .await?;

    drive(
        DeleteMetadataDocumentOperation::new(
            Actor {
                node_id: nodes[0].net.node_id(),
                user_id: Ulid::new(),
                realm_id,
            },
            group_id,
            document_id,
        ),
        nodes[0].context.as_ref(),
    )
    .await?;

    wait_for_metadata_absence(&nodes[1..], group_id, document_id, &created.graph_iri).await?;
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
    let metadata_handle = MetadataHandle::new(
        temp_dir.path().join("metadata"),
        net.node_id(),
        storage.clone(),
        Some(net.clone()),
    )?;

    let context = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: Some(net.clone()),
        blob_handle: None,
        automerge_handle: None,
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
                GetRealmNodesOperation::new(realm_id.clone()),
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

async fn wait_for_metadata_convergence(
    nodes: &[TestNode],
    group_id: Ulid,
    document_id: Ulid,
    graph_iri: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    wait_for_metadata_state(
        nodes,
        group_id,
        document_id,
        graph_iri,
        nodes.len(),
        "Bootstrap Dataset",
    )
    .await
}

async fn wait_for_metadata_state(
    nodes: &[TestNode],
    group_id: Ulid,
    document_id: Ulid,
    graph_iri: &str,
    expected_holder_count: usize,
    expected_text: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    let mut last_states = Vec::new();

    loop {
        let mut converged = true;
        last_states.clear();
        for node in nodes {
            match drive(
                GetMetadataDocumentOperation::new(group_id, document_id),
                node.context.as_ref(),
            )
            .await
            {
                Ok(document)
                    if document.record.graph_iri == graph_iri
                        && document.record.holder_node_ids.len() == expected_holder_count
                        && document.jsonld.contains(expected_text) =>
                {
                    last_states.push(format!("node={} converged", node.net.node_id()));
                }
                Ok(document) => {
                    last_states.push(format!(
                        "node={} graph={} holders={} jsonld_contains={}",
                        node.net.node_id(),
                        document.record.graph_iri,
                        document.record.holder_node_ids.len(),
                        document.jsonld.contains(expected_text)
                    ));
                    converged = false;
                    break;
                }
                Err(error) => {
                    let graph_state = match node
                        .context
                        .metadata_handle
                        .as_ref()
                        .unwrap()
                        .send_effect(Effect::Metadata(MetadataEffect::ExportRoCrate {
                            graph_iri: graph_iri.to_string(),
                        }))
                        .await
                    {
                        Event::Metadata(MetadataEvent::RoCrateExportResult { .. }) => {
                            "graph-present"
                        }
                        Event::Metadata(MetadataEvent::Error { .. }) => "graph-missing",
                        _ => "graph-unknown",
                    };
                    last_states.push(format!(
                        "node={} error={error:?} {graph_state}",
                        node.net.node_id()
                    ));
                    converged = false;
                    break;
                }
            }
        }

        if converged {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("metadata state did not converge: {last_states:?}").into());
        }
        sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_metadata_absence(
    nodes: &[TestNode],
    group_id: Ulid,
    document_id: Ulid,
    graph_iri: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    let mut last_states = Vec::new();

    loop {
        let mut converged = true;
        last_states.clear();
        for node in nodes {
            match drive(
                GetMetadataDocumentOperation::new(group_id, document_id),
                node.context.as_ref(),
            )
            .await
            {
                Err(_) => {
                    let graph_state = match node
                        .context
                        .metadata_handle
                        .as_ref()
                        .unwrap()
                        .send_effect(Effect::Metadata(MetadataEffect::ExportRoCrate {
                            graph_iri: graph_iri.to_string(),
                        }))
                        .await
                    {
                        Event::Metadata(MetadataEvent::Error { .. }) => "graph-missing",
                        _ => "graph-present",
                    };
                    if graph_state != "graph-missing" {
                        last_states
                            .push(format!("node={} graph still present", node.net.node_id()));
                        converged = false;
                        break;
                    }
                    last_states.push(format!("node={} absent", node.net.node_id()));
                }
                Ok(_) => {
                    last_states.push(format!(
                        "node={} document still present",
                        node.net.node_id()
                    ));
                    converged = false;
                    break;
                }
            }
        }

        if converged {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("metadata deletion did not converge: {last_states:?}").into());
        }
        sleep(Duration::from_millis(50)).await;
    }
}

async fn shutdown_nodes(nodes: Vec<TestNode>) {
    for node in nodes {
        node.net.shutdown().await;
    }
}
