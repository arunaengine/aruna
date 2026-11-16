//! Ignored load test that documents written at a steady pace under load converge on every node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use aruna_core::NodeId;
use aruna_core::StructuredId;
use aruna_core::UserId;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::types::GroupId;
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::metadata::MetadataHandle;
use aruna_operations::metadata::create_document::{
    CreateDocumentConfig, CreateDocumentOperation, CreateDocumentPayload, mint_local_document,
};
use aruna_operations::metadata::projector::project_logged_events;
use aruna_operations::realm::announce_presence::{
    AnnouncePresenceConfig, AnnouncePresenceOperation,
};
use aruna_operations::realm::get_config::GetConfigOperation;
use aruna_operations::realm::get_nodes::GetNodesOperation;
use aruna_operations::sync::incoming::initialize_incoming_fixture;
use aruna_operations::tasks::incoming::start_task_queues;
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use tempfile::TempDir;
use ulid::Ulid;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

use crate::convergence;
use crate::topology::mesh;
use crate::topology::store_realm_config;
use convergence::{wait_empty_queues, wait_for_convergence, wait_for_visibility};

const PROJECTION_BATCH: usize = 32;

const WRITERS: usize = 24;
const DOCS_PER_WRITER: usize = 500;
const WRITER_PERIOD: Duration = Duration::from_millis(40);

struct TestNode {
    _temp_dir: Option<TempDir>,
    net: NetHandle,
    _task_handle: TaskHandle,
    context: Arc<DriverContext>,
}

fn init_logging() {
    if std::env::var("RUST_LOG").is_ok() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
            .try_init();
    }
}

fn make_runtime() -> Result<tokio::runtime::Runtime, BoxError> {
    Ok(tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?)
}

// The load of the propagation tail seen in the 3-node cluster. Its latency
// percentiles are measured by the measure example.
#[test]
#[ignore]
fn sustained_load_converges() -> Result<(), BoxError> {
    init_logging();
    let runtime = make_runtime()?;
    runtime.block_on(async {
        let realm_id = RealmId([125u8; 32]);
        let nodes = build_realm_nodes(&realm_id, 3).await?;
        let targets = node_targets(&nodes);
        let contexts: Vec<Arc<DriverContext>> = nodes.iter().map(|n| n.context.clone()).collect();
        let group_id = Ulid::generate();

        let mut handles = Vec::with_capacity(WRITERS);
        for writer in 0..WRITERS {
            let targets = targets.clone();
            handles.push(tokio::spawn(async move {
                run_paced_writer(realm_id, group_id, "tail", writer, DOCS_PER_WRITER, targets).await
            }));
        }
        let mut created: Vec<(GroupId, Ulid, Instant)> = Vec::new();
        for handle in handles {
            created.extend(handle.await??);
        }
        let pairs: Vec<(GroupId, Ulid)> = created
            .iter()
            .map(|(group_id, document_id, _)| (*group_id, *document_id))
            .collect();
        let result = match wait_for_visibility(&contexts, &pairs, Duration::from_millis(200)).await
        {
            Ok(()) => wait_empty_queues(&contexts).await,
            Err(error) => Err(error),
        };
        shutdown_nodes(nodes).await;
        Ok::<(), BoxError>(result?)
    })?;
    runtime.shutdown_timeout(Duration::from_secs(10));
    Ok(())
}

fn node_targets(nodes: &[TestNode]) -> Vec<(NodeId, Arc<DriverContext>)> {
    nodes
        .iter()
        .map(|node| (node.net.node_id(), node.context.clone()))
        .collect()
}

fn scaffold_payload(label: &str, writer: usize, index: usize) -> CreateDocumentPayload {
    CreateDocumentPayload::Scaffold {
        name: format!("Bench Dataset {label}-{writer}-{index}"),
        description: "Propagation tail benchmark document".to_string(),
        date_published: "2026-06-10".to_string(),
        license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
    }
}

fn rocrate_payload(document_id: Ulid) -> CreateDocumentPayload {
    let jsonld = format!(
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
      "name": "Bench Crate {document_id}",
      "description": "Propagation tail benchmark crate",
      "datePublished": "2026-06-10",
      "license": {{"@id": "https://creativecommons.org/licenses/by/4.0/"}}
    }}
  ]
}}"#
    );
    CreateDocumentPayload::RoCrate { jsonld }
}

// Same per-document create path as run_writer in metadata_throughput.rs, but
// paced with a tokio interval so each writer issues ~25 creates/s.
async fn run_paced_writer(
    realm_id: RealmId,
    group_id: GroupId,
    label: &str,
    writer: usize,
    count: usize,
    targets: Vec<(NodeId, Arc<DriverContext>)>,
) -> Result<Vec<(GroupId, Ulid, Instant)>, BoxError> {
    let mut ticker = tokio::time::interval(WRITER_PERIOD);
    let mut batches: Vec<Vec<(Ulid, Ulid)>> = targets.iter().map(|_| Vec::new()).collect();
    let mut pending = 0usize;
    let mut created = Vec::with_capacity(count);
    let config = drive(GetConfigOperation::new(realm_id), targets[0].1.as_ref())
        .await
        .map_err(|error| format!("realm config load failed: {error:?}"))?;

    for index in 0..count {
        ticker.tick().await;
        let slot = (writer + index) % targets.len();
        let (node_id, context) = &targets[slot];
        let document_path = format!("datasets/bench-{label}-{writer}-{index}");
        let actor = Actor {
            node_id: *node_id,
            user_id: UserId::local(Ulid::generate(), realm_id),
            realm_id,
        };
        let document_id = mint_local_document(&config, &actor, group_id, &document_path)
            .map_err(|error| format!("mint failed writer={writer} index={index}: {error:?}"))?
            .as_ulid();
        let payload = if index % 2 == 0 {
            scaffold_payload(label, writer, index)
        } else {
            rocrate_payload(document_id)
        };
        let result = drive(
            CreateDocumentOperation::new_generated_id(CreateDocumentConfig {
                actor,
                group_id,
                document_id,
                document_path,
                public: true,
                payload,
            }),
            context.as_ref(),
        )
        .await
        .map_err(|error| format!("create failed writer={writer} index={index}: {error:?}"))?;

        batches[slot].push((result.record.document_id, result.record.last_event_id));
        created.push((group_id, result.record.document_id, Instant::now()));
        pending += 1;
        if pending >= PROJECTION_BATCH {
            flush_projection_batches(&targets, &mut batches).await?;
            pending = 0;
        }
    }
    flush_projection_batches(&targets, &mut batches).await?;
    Ok(created)
}

async fn flush_projection_batches(
    targets: &[(NodeId, Arc<DriverContext>)],
    batches: &mut [Vec<(Ulid, Ulid)>],
) -> Result<(), BoxError> {
    for (slot, batch) in batches.iter_mut().enumerate() {
        if batch.is_empty() {
            continue;
        }
        let drained: Vec<(Ulid, Ulid)> = std::mem::take(batch);
        project_logged_events(targets[slot].1.as_ref(), drained)
            .await
            .map_err(|error| format!("projection failed: {error:?}"))?;
    }
    Ok(())
}

async fn build_realm_nodes(realm_id: &RealmId, count: usize) -> Result<Vec<TestNode>, BoxError> {
    let mut nodes = Vec::with_capacity(count);
    for _ in 0..count {
        nodes.push(spawn_node(*realm_id).await?);
    }
    mesh(nodes.iter().map(|node| &node.net)).await;

    for node in &nodes {
        drive(
            AnnouncePresenceOperation::new(AnnouncePresenceConfig {
                realm_id: *realm_id,
                node_id: node.net.node_id(),
                schedule_refresh: true,
            }),
            node.context.as_ref(),
        )
        .await?;
    }

    wait_node_convergence(&nodes, realm_id).await?;
    install_realm_config(&nodes, realm_id).await?;
    Ok(nodes)
}

async fn spawn_node(realm_id: RealmId) -> Result<TestNode, BoxError> {
    let temp_dir = tempfile::tempdir()?;
    let mut node = spawn_node_with(realm_id, None, temp_dir.path().to_path_buf()).await?;
    node._temp_dir = Some(temp_dir);
    Ok(node)
}

async fn spawn_node_with(
    realm_id: RealmId,
    secret_key: Option<iroh::SecretKey>,
    dir: PathBuf,
) -> Result<TestNode, BoxError> {
    let fjall_dir = dir.join("fjall");
    std::fs::create_dir_all(&fjall_dir)?;
    let storage = FjallStorage::open(fjall_dir.to_str().ok_or("invalid storage path")?)?;
    let net = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().expect("valid bind addr"),
            secret_key,
            realm_id,
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            sync_storage_path: Some(dir.join("document-sync")),
            ..NetConfig::default()
        },
        storage.clone(),
    )
    .await?;
    let task_handle = TaskHandle::new();
    let metadata_handle = MetadataHandle::new(
        dir.join("metadata"),
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
        compute_handle: None,
    });

    initialize_incoming_fixture(context.clone());
    let shutdown = aruna_core::shutdown::Shutdown::new();
    start_task_queues(
        context.clone(),
        task_handle.clone(),
        aruna_operations::jobs::runtime::JobsRuntime::new(),
        &shutdown,
    )
    .await;

    Ok(TestNode {
        _temp_dir: None,
        net,
        _task_handle: task_handle,
        context,
    })
}

async fn install_realm_config(nodes: &[TestNode], realm_id: &RealmId) -> Result<(), BoxError> {
    let mut config = RealmConfigDocument::default_for_realm(*realm_id, Vec::new());
    config.seed_default_placement();
    for (band, node) in nodes.iter().enumerate() {
        config.ensure_node(node.net.node_id(), RealmNodeKind::Management);
        config.seed_job_control(node.net.node_id(), band as u32);
    }

    for node in nodes {
        store_realm_config(&node.net, &node.context, *realm_id, &config).await?;
    }
    // Config apply hook: the shard's rank-0 holder eagerly creates each
    // shard topic genesis (mirrors the production realm-config apply path).
    for node in nodes {
        aruna_operations::placement::process_placements::process_shard_placements(
            &node.context,
            *realm_id,
            node.net.node_id(),
        )
        .await;
    }
    Ok(())
}

async fn wait_node_convergence(nodes: &[TestNode], realm_id: &RealmId) -> Result<(), BoxError> {
    let expected: HashSet<_> = nodes.iter().map(|node| node.net.node_id()).collect();
    wait_for_convergence("realm nodes did not converge", || async {
        let mut pending = 0;
        for node in nodes {
            match drive(GetNodesOperation::new(*realm_id), node.context.as_ref()).await {
                Ok(realm_nodes) if realm_nodes == expected => {}
                _ => pending += 1,
            }
        }
        Ok(pending)
    })
    .await
}

async fn shutdown_nodes(nodes: Vec<TestNode>) {
    for node in nodes {
        node.net.shutdown().await;
    }
}
