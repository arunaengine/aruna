//! Times fixed workloads through the runner on real backends in temporary folders and prints rates.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Lints overflow the default query depth on the nested async writer tasks, as in tests/main.rs.
#![recursion_limit = "512"]

#[path = "../tests/convergence/mod.rs"]
mod convergence;
#[path = "../tests/topology/mod.rs"]
mod topology;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use aruna_core::shutdown::Shutdown;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::types::GroupId;
use aruna_core::{NodeId, StructuredId, UserId};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::jobs::runtime::JobsRuntime;
use aruna_operations::metadata::MetadataHandle;
use aruna_operations::metadata::create_document::{
    CreateDocumentConfig, CreateDocumentOperation, CreateDocumentPayload, mint_local_document,
};
use aruna_operations::metadata::projector::project_logged_events;
use aruna_operations::placement::process_placements::process_shard_placements;
use aruna_operations::realm::announce_presence::{
    AnnouncePresenceConfig, AnnouncePresenceOperation,
};
use aruna_operations::realm::get_nodes::GetNodesOperation;
use aruna_operations::sync::incoming::initialize_incoming_fixture;
use aruna_operations::tasks::incoming::start_task_queues;
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use convergence::{TestResult, wait_empty_queues, wait_for_convergence, wait_for_visibility};
use tempfile::TempDir;
use topology::store_realm_config;
use ulid::Ulid;

const REALM_NODES: usize = 3;
const TOTAL_CREATES: usize = 2000;
const WRITER_LEVELS: [usize; 4] = [1, 8, 32, 64];
const PROJECTION_BATCH: usize = 32;
const SPREAD_WRITERS: usize = 64;
const SPREAD_DOCS: usize = 16;
const SAMPLE_DOCS: usize = 100;
const DRAIN_WRITERS: usize = 32;
const DRAIN_DOCS: usize = 128;
const VISIBILITY_POLL: Duration = Duration::from_millis(200);

type Created = (GroupId, Ulid, Instant);

/// A node's temporary folder, net handle and runner context, kept alive together.
struct Node {
    _dir: TempDir,
    _shutdown: Shutdown,
    net: NetHandle,
    context: Arc<DriverContext>,
}

struct Realm {
    realm_id: RealmId,
    config: RealmConfigDocument,
    nodes: Vec<Node>,
}

/// What every writer task shares: the realm, the target nodes and the group it writes to.
struct Workload {
    realm_id: RealmId,
    config: RealmConfigDocument,
    group_id: GroupId,
    targets: Vec<(NodeId, Arc<DriverContext>)>,
}

fn main() -> TestResult<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(run_all());
    runtime.shutdown_timeout(Duration::from_secs(10));
    result
}

async fn run_all() -> TestResult<()> {
    for (level, writers) in WRITER_LEVELS.into_iter().enumerate() {
        measure_creates(91 + level as u8, writers).await?;
    }
    measure_spread().await?;
    measure_drain().await?;
    Ok(())
}

fn report(workload: &str, detail: &str, count: usize, seconds: f64) {
    let rate = count as f64 / seconds;
    println!("{workload} {detail} count={count} seconds={seconds:.3} per_second={rate:.1}");
}

/// Creates and projects TOTAL_CREATES documents with `writers` parallel writers.
async fn measure_creates(seed: u8, writers: usize) -> TestResult<()> {
    let realm = build_realm(seed).await?;
    let started = Instant::now();
    let created = write_all(&realm, writers, TOTAL_CREATES / writers).await;
    let seconds = started.elapsed().as_secs_f64();
    shutdown_realm(realm).await;
    report(
        "metadata_create",
        &format!("writers={writers}"),
        created?.len(),
        seconds,
    );
    Ok(())
}

/// Time from the last create until the last SAMPLE_DOCS documents read back on every node.
async fn measure_spread() -> TestResult<()> {
    let realm = build_realm(122).await?;
    let mut created = write_all(&realm, SPREAD_WRITERS, SPREAD_DOCS).await?;
    created.sort_by_key(|(_, _, at)| *at);
    let sample: Vec<(GroupId, Ulid)> = created
        .iter()
        .rev()
        .take(SAMPLE_DOCS)
        .map(|(group_id, document_id, _)| (*group_id, *document_id))
        .collect();
    let started = Instant::now();
    let result = wait_for_visibility(&contexts(&realm), &sample, VISIBILITY_POLL).await;
    let seconds = started.elapsed().as_secs_f64();
    shutdown_realm(realm).await;
    result?;
    report(
        "metadata_spread",
        &format!("created={}", created.len()),
        sample.len(),
        seconds,
    );
    Ok(())
}

/// Time from the first create until every document is visible everywhere and no
/// materialization job is left, through the production task queues.
async fn measure_drain() -> TestResult<()> {
    let realm = build_realm(124).await?;
    let started = Instant::now();
    let created = write_all(&realm, DRAIN_WRITERS, DRAIN_DOCS).await?;
    let pairs: Vec<(GroupId, Ulid)> = created
        .iter()
        .map(|(group_id, document_id, _)| (*group_id, *document_id))
        .collect();
    let all = contexts(&realm);
    let result = match wait_for_visibility(&all, &pairs, VISIBILITY_POLL).await {
        Ok(()) => wait_empty_queues(&all).await,
        Err(error) => Err(error),
    };
    let seconds = started.elapsed().as_secs_f64();
    shutdown_realm(realm).await;
    result?;
    report(
        "metadata_converge",
        &format!("writers={DRAIN_WRITERS}"),
        pairs.len(),
        seconds,
    );
    Ok(())
}

/// Runs `writers` tasks with `per_writer` creates each and returns every created document.
async fn write_all(realm: &Realm, writers: usize, per_writer: usize) -> TestResult<Vec<Created>> {
    let workload = Arc::new(Workload {
        realm_id: realm.realm_id,
        config: realm.config.clone(),
        group_id: Ulid::generate(),
        targets: realm
            .nodes
            .iter()
            .map(|node| (node.net.node_id(), node.context.clone()))
            .collect(),
    });
    let mut handles = Vec::with_capacity(writers);
    for writer in 0..writers {
        let workload = workload.clone();
        handles.push(tokio::spawn(async move {
            workload.write(writer, per_writer).await
        }));
    }
    let mut created = Vec::with_capacity(writers * per_writer);
    for handle in handles {
        created.extend(handle.await??);
    }
    Ok(created)
}

impl Workload {
    /// Creates `count` documents round robin over the nodes, alternating a scaffold
    /// and an RO-Crate payload, and projects them in batches as the API path does.
    async fn write(&self, writer: usize, count: usize) -> Result<Vec<Created>, String> {
        let mut batches: Vec<Vec<(Ulid, Ulid)>> = self.targets.iter().map(|_| Vec::new()).collect();
        let mut pending = 0usize;
        let mut created = Vec::with_capacity(count);
        for index in 0..count {
            let slot = (writer + index) % self.targets.len();
            let (node_id, context) = &self.targets[slot];
            let document_path = format!("datasets/measure-{writer}-{index}");
            let actor = Actor {
                node_id: *node_id,
                user_id: UserId::local(Ulid::generate(), self.realm_id),
                realm_id: self.realm_id,
            };
            let document_id =
                mint_local_document(&self.config, &actor, self.group_id, &document_path)
                    .map_err(|error| format!("mint failed: {error:?}"))?
                    .as_ulid();
            let payload = if index % 2 == 0 {
                scaffold_document(writer, index)
            } else {
                crate_document(document_id)
            };
            let config = CreateDocumentConfig {
                actor,
                group_id: self.group_id,
                document_id,
                document_path,
                public: true,
                payload,
            };
            let result = drive(
                CreateDocumentOperation::new_generated_id(config),
                context.as_ref(),
            )
            .await
            .map_err(|error| format!("create failed: {error:?}"))?;
            batches[slot].push((result.record.document_id, result.record.last_event_id));
            created.push((self.group_id, result.record.document_id, Instant::now()));
            pending += 1;
            if pending >= PROJECTION_BATCH {
                self.project(&mut batches).await?;
                pending = 0;
            }
        }
        self.project(&mut batches).await?;
        Ok(created)
    }

    async fn project(&self, batches: &mut [Vec<(Ulid, Ulid)>]) -> Result<(), String> {
        for (slot, batch) in batches.iter_mut().enumerate() {
            if !batch.is_empty() {
                project_logged_events(self.targets[slot].1.as_ref(), std::mem::take(batch))
                    .await
                    .map_err(|error| format!("projection failed: {error:?}"))?;
            }
        }
        Ok(())
    }
}

fn scaffold_document(writer: usize, index: usize) -> CreateDocumentPayload {
    CreateDocumentPayload::Scaffold {
        name: format!("Measured Dataset {writer}-{index}"),
        description: "Measurement harness document".to_string(),
        date_published: "2026-11-16".to_string(),
        license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
    }
}

fn crate_document(document_id: Ulid) -> CreateDocumentPayload {
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
      "name": "Measured Crate {document_id}",
      "description": "Measurement harness crate",
      "datePublished": "2026-11-16",
      "license": {{"@id": "https://creativecommons.org/licenses/by/4.0/"}}
    }}
  ]
}}"#
    );
    CreateDocumentPayload::RoCrate { jsonld }
}

fn contexts(realm: &Realm) -> Vec<Arc<DriverContext>> {
    realm
        .nodes
        .iter()
        .map(|node| node.context.clone())
        .collect()
}

/// Spawns REALM_NODES meshed nodes, waits until they see each other and installs
/// a realm config with every node as a management node, as a bootstrapped realm has.
async fn build_realm(seed: u8) -> TestResult<Realm> {
    let realm_id = RealmId([seed; 32]);
    let mut nodes = Vec::with_capacity(REALM_NODES);
    for _ in 0..REALM_NODES {
        nodes.push(spawn_node(realm_id).await?);
    }
    for left in 0..nodes.len() {
        for right in (left + 1)..nodes.len() {
            nodes[left]
                .net
                .add_peer_addr(nodes[right].net.endpoint_addr())
                .await;
            nodes[right]
                .net
                .add_peer_addr(nodes[left].net.endpoint_addr())
                .await;
        }
    }
    for node in &nodes {
        let announce = AnnouncePresenceConfig {
            realm_id,
            node_id: node.net.node_id(),
            schedule_refresh: true,
        };
        drive(
            AnnouncePresenceOperation::new(announce),
            node.context.as_ref(),
        )
        .await
        .map_err(|error| format!("announce failed: {error:?}"))?;
    }
    let expected: HashSet<NodeId> = nodes.iter().map(|node| node.net.node_id()).collect();
    wait_for_convergence::<_, _, String>("realm nodes did not converge", || async {
        let mut pending = 0;
        for node in &nodes {
            match drive(GetNodesOperation::new(realm_id), node.context.as_ref()).await {
                Ok(seen) if seen == expected => {}
                _ => pending += 1,
            }
        }
        Ok(pending)
    })
    .await?;
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.seed_default_placement();
    for (band, node) in nodes.iter().enumerate() {
        config.ensure_node(node.net.node_id(), RealmNodeKind::Management);
        config.seed_job_control(node.net.node_id(), band as u32);
    }
    for node in &nodes {
        store_realm_config(&node.net, &node.context, realm_id, &config).await?;
    }
    for node in &nodes {
        process_shard_placements(&node.context, realm_id, node.net.node_id()).await;
    }
    Ok(Realm {
        realm_id,
        config,
        nodes,
    })
}

async fn spawn_node(realm_id: RealmId) -> TestResult<Node> {
    let dir = tempfile::tempdir()?;
    let fjall_dir = dir.path().join("fjall");
    std::fs::create_dir_all(&fjall_dir)?;
    let storage = FjallStorage::open(fjall_dir.to_str().ok_or("invalid storage path")?)?;
    let net_config = NetConfig {
        bind_addr: "127.0.0.1:0".parse()?,
        realm_id,
        discovery_method: DiscoveryMethod::None,
        relay_method: RelayMethod::None,
        sync_storage_path: Some(dir.path().join("document-sync")),
        ..NetConfig::default()
    };
    let net = NetHandle::new(net_config, storage.clone()).await?;
    let task_handle = TaskHandle::new();
    let metadata_handle = MetadataHandle::new(
        dir.path().join("metadata"),
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
    let shutdown = Shutdown::new();
    start_task_queues(context.clone(), task_handle, JobsRuntime::new(), &shutdown).await;
    Ok(Node {
        _dir: dir,
        _shutdown: shutdown,
        net,
        context,
    })
}

async fn shutdown_realm(realm: Realm) {
    for node in realm.nodes {
        node.net.shutdown().await;
    }
}
