// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "256"]
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aruna_core::StructuredId;
use aruna_core::UserId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::structs::{Actor, RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::types::GroupId;
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::announce_realm_presence::{
    AnnounceRealmPresenceConfig, AnnounceRealmPresenceOperation,
};
use aruna_operations::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentOperation, CreateMetadataDocumentPayload,
    mint_local_document,
};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::get_metadata_document::GetMetadataDocumentOperation;
use aruna_operations::get_realm_config::GetRealmConfigOperation;
use aruna_operations::get_realm_nodes::GetRealmNodesOperation;
use aruna_operations::incoming::initialize_net_holder;
use aruna_operations::metadata::MetadataHandle;
use aruna_operations::metadata::projector::project_metadata_create_events_from_log;
use aruna_operations::startup::{SHARED_RESTORE_TOPIC_COUNT, restore_shard_subscriptions};
use aruna_operations::task_incoming::initialize_task_incoming;
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use tempfile::TempDir;
use tokio::time::sleep;
use ulid::Ulid;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

mod convergence;
use convergence::{HANG_CAP, NO_PROGRESS_TIMEOUT, wait_for_convergence};

const PROJECTION_BATCH: usize = 32;
const SEED_DOCUMENTS: usize = 500;

struct TestNode {
    _temp_dir: Option<TempDir>,
    net: NetHandle,
    task_handle: TaskHandle,
    context: Arc<DriverContext>,
    shutdown: aruna_core::shutdown::Shutdown,
}

struct IncidentFixture {
    nodes: Vec<TestNode>,
    secrets: [iroh::SecretKey; 3],
    config: RealmConfigDocument,
    target: aruna_core::document::DocumentSyncTarget,
    placement: aruna_core::structs::PlacementRef,
}

struct OutageFixture {
    live: TestNode,
    dir_one: TempDir,
    dir_two: TempDir,
    secrets: [iroh::SecretKey; 3],
    config: RealmConfigDocument,
    target: aruna_core::document::DocumentSyncTarget,
    placement: aruna_core::structs::PlacementRef,
    group_id: GroupId,
    document_id: Ulid,
    seeded: Vec<Vec<u8>>,
}

// A restart re-announces one topic per held shard plus the fixed shared topics —
// never one per stored document — and a fresh write still converges to the
// restarted node afterwards.
#[test]
fn restart_reannounces_held_shard_topics_not_documents() -> Result<(), BoxError> {
    let runtime = make_runtime()?;
    let result = runtime.block_on(restart_traffic_body());
    runtime.shutdown_timeout(Duration::from_secs(10));
    result
}

async fn restart_traffic_body() -> Result<(), BoxError> {
    let realm_id = RealmId([126u8; 32]);
    let node2_dir = tempfile::tempdir()?;
    let secret = iroh::SecretKey::from_bytes(&[7u8; 32]);

    let aux = AuxRuntime::new()?;
    let (mut nodes, group_id, created) =
        prepare_restart_state(realm_id, &node2_dir, &secret, &aux).await?;
    let node2 = restart_node(realm_id, &mut nodes, &node2_dir, secret, aux).await?;
    assert_restore_bound(&node2, realm_id, &created).await;
    nodes.push(node2);
    finish_restart(nodes, realm_id, group_id).await
}

async fn prepare_restart_state(
    realm_id: RealmId,
    node2_dir: &TempDir,
    secret: &iroh::SecretKey,
    aux: &AuxRuntime,
) -> Result<(Vec<TestNode>, GroupId, Vec<(GroupId, Ulid)>), BoxError> {
    let nodes = spawn_restart_nodes(realm_id, node2_dir, secret, aux).await?;
    announce_restart_nodes(&nodes, realm_id, aux).await?;
    wait_for_realm_node_convergence(&nodes, &realm_id).await?;
    install_realm_config(&nodes, &realm_id).await?;
    let (group_id, created) = seed_restart_documents(realm_id, &nodes).await?;
    Ok((nodes, group_id, created))
}

async fn assert_restore_bound(node: &TestNode, realm_id: RealmId, created: &[(GroupId, Ulid)]) {
    // The startup restore touches O(held shards) topics, not O(documents).
    let summary = restore_shard_subscriptions(&node.context, node.net.node_id(), realm_id).await;
    println!(
        "restore summary: held_shards={} shard_topics={} shared_topics={} total={}",
        summary.held_shards,
        summary.shard_topics,
        summary.shared_topics,
        summary.total_topics()
    );
    assert!(summary.held_shards > 0, "node 2 must hold shards");
    assert_eq!(summary.shared_topics, SHARED_RESTORE_TOPIC_COUNT);
    assert!(
        summary.total_topics() <= summary.held_shards + SHARED_RESTORE_TOPIC_COUNT,
        "restore announced {} topics, more than held_shards {} + shared {}",
        summary.total_topics(),
        summary.held_shards,
        SHARED_RESTORE_TOPIC_COUNT
    );
    assert!(
        summary.total_topics() < created.len(),
        "restore announced {} topics, not fewer than the {} seeded documents",
        summary.total_topics(),
        created.len()
    );
}

async fn finish_restart(
    nodes: Vec<TestNode>,
    realm_id: RealmId,
    group_id: GroupId,
) -> Result<(), BoxError> {
    // A fresh write after the restart converges to all nodes.
    let fresh = run_writer(realm_id, group_id, 1, "fresh", {
        vec![(nodes[0].net.node_id(), nodes[0].context.clone())]
    })
    .await?;
    let contexts: Vec<Arc<DriverContext>> = nodes.iter().map(|node| node.context.clone()).collect();
    wait_for_visibility(&contexts, &fresh, Duration::from_millis(200)).await?;
    println!("fresh write converged to all nodes after restart");

    shutdown_nodes(nodes).await;
    Ok(())
}

async fn spawn_restart_nodes(
    realm_id: RealmId,
    node2_dir: &TempDir,
    secret: &iroh::SecretKey,
    aux: &AuxRuntime,
) -> Result<Vec<TestNode>, BoxError> {
    let mut nodes = Vec::with_capacity(3);
    nodes.push(spawn_node(realm_id).await?);
    nodes.push(spawn_node(realm_id).await?);
    let dir2 = node2_dir.path().to_path_buf();
    let secret2 = secret.clone();
    let node2 = aux
        .handle()
        .spawn(async move { spawn_node_with(realm_id, Some(secret2), dir2).await })
        .await??;
    nodes.push(node2);
    wire_peers(&nodes).await;
    Ok(nodes)
}

async fn announce_restart_nodes(
    nodes: &[TestNode],
    realm_id: RealmId,
    aux: &AuxRuntime,
) -> Result<(), BoxError> {
    for (index, node) in nodes.iter().enumerate() {
        let op = AnnounceRealmPresenceOperation::new(AnnounceRealmPresenceConfig {
            realm_id,
            node_id: node.net.node_id(),
            schedule_refresh: true,
        });
        if index == 2 {
            let ctx = node.context.clone();
            aux.handle()
                .spawn(async move {
                    drive(op, ctx.as_ref())
                        .await
                        .map_err(|error| format!("announce failed: {error:?}"))
                })
                .await??;
        } else {
            drive(op, node.context.as_ref()).await?;
        }
    }
    Ok(())
}

async fn seed_restart_documents(
    realm_id: RealmId,
    nodes: &[TestNode],
) -> Result<(GroupId, Vec<(GroupId, Ulid)>), BoxError> {
    let group_id = Ulid::generate();
    let targets = vec![(nodes[0].net.node_id(), nodes[0].context.clone())];
    let created = run_writer(realm_id, group_id, SEED_DOCUMENTS, "seed", targets).await?;
    let sample: Vec<(GroupId, Ulid)> = created
        .iter()
        .rev()
        .take(40)
        .map(|(group_id, document_id)| (*group_id, *document_id))
        .collect();
    wait_sample_visible(&nodes[2].context, &sample).await?;
    println!(
        "seeded {} docs, node 2 holds replicated state",
        created.len()
    );
    Ok((group_id, created))
}

async fn restart_node(
    realm_id: RealmId,
    nodes: &mut Vec<TestNode>,
    node2_dir: &TempDir,
    secret: iroh::SecretKey,
    aux: AuxRuntime,
) -> Result<TestNode, BoxError> {
    let node2 = nodes.pop().ok_or("node 2 present")?;
    let _ = node2.shutdown.drain(NO_PROGRESS_TIMEOUT).await;
    node2.net.clear_inbound_handler();
    let _ = node2.task_handle.shutdown(NO_PROGRESS_TIMEOUT).await;
    node2.net.shutdown().await;
    drop(node2);
    aux.shutdown().await?;
    println!("node 2 shut down");

    let node2 = respawn_with_retry(realm_id, secret, node2_dir.path()).await?;
    for other in nodes {
        node2.net.add_peer_addr(other.net.endpoint_addr()).await;
        other.net.add_peer_addr(node2.net.endpoint_addr()).await;
    }
    drive(
        AnnounceRealmPresenceOperation::new(AnnounceRealmPresenceConfig {
            realm_id,
            node_id: node2.net.node_id(),
            schedule_refresh: true,
        }),
        node2.context.as_ref(),
    )
    .await?;
    Ok(node2)
}

fn make_runtime() -> Result<tokio::runtime::Runtime, BoxError> {
    Ok(tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?)
}

// Owns the auxiliary node-2 runtime and always tears it down off the async
// context. Shutting a runtime down by dropping it inside an async task panics;
// routing every drop through `spawn_blocking` means an early `?` return yields
// the real error instead of that masking panic.
struct AuxRuntime(Option<tokio::runtime::Runtime>);

impl AuxRuntime {
    fn new() -> Result<Self, BoxError> {
        Ok(Self(Some(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()?,
        )))
    }

    fn handle(&self) -> tokio::runtime::Handle {
        self.0
            .as_ref()
            .expect("aux runtime present")
            .handle()
            .clone()
    }

    async fn shutdown(mut self) -> Result<(), BoxError> {
        if let Some(runtime) = self.0.take() {
            let shutdown = tokio::task::spawn_blocking(move || drop(runtime));
            tokio::time::timeout(HANG_CAP, shutdown)
                .await
                .map_err(|_| "aux runtime shutdown timed out")?
                .map_err(|error| format!("aux runtime shutdown failed: {error}"))?;
        }
        Ok(())
    }
}

impl Drop for AuxRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = self.0.take() {
            tokio::task::spawn_blocking(move || runtime.shutdown_timeout(Duration::from_secs(10)));
        }
    }
}

async fn run_writer(
    realm_id: RealmId,
    group_id: GroupId,
    count: usize,
    label: &str,
    targets: Vec<(aruna_core::NodeId, Arc<DriverContext>)>,
) -> Result<Vec<(GroupId, Ulid)>, BoxError> {
    let mut batches: Vec<Vec<(Ulid, Ulid)>> = targets.iter().map(|_| Vec::new()).collect();
    let mut pending = 0usize;
    let mut created = Vec::with_capacity(count);

    // Mint from the replicated realm config; the id is no longer in the path,
    // since the path now feeds the bucket the id embeds.
    let config = drive(
        GetRealmConfigOperation::new(realm_id),
        targets[0].1.as_ref(),
    )
    .await
    .map_err(|error| format!("realm config load failed: {error:?}"))?;

    for index in 0..count {
        let slot = index % targets.len();
        let (node_id, context) = &targets[slot];
        let document_path = format!("datasets/restart-{label}-{index}");
        let actor = Actor {
            node_id: *node_id,
            user_id: UserId::local(Ulid::generate(), realm_id),
            realm_id,
        };
        let document_id = mint_local_document(&config, &actor, group_id, &document_path)
            .map_err(|error| format!("mint failed index={index}: {error:?}"))?
            .as_ulid();
        let result = drive(
            CreateMetadataDocumentOperation::new_for_generated_document_id(
                CreateMetadataDocumentConfig {
                    actor,
                    group_id,
                    document_id,
                    document_path,
                    public: true,
                    payload: CreateMetadataDocumentPayload::Scaffold {
                        name: format!("Restart Dataset {index}"),
                        description: "Restart traffic document".to_string(),
                        date_published: "2026-07-07".to_string(),
                        license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                    },
                },
            ),
            context.as_ref(),
        )
        .await
        .map_err(|error| format!("create failed index={index}: {error:?}"))?;

        batches[slot].push((result.record.document_id, result.record.last_event_id));
        created.push((group_id, result.record.document_id));
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
    targets: &[(aruna_core::NodeId, Arc<DriverContext>)],
    batches: &mut [Vec<(Ulid, Ulid)>],
) -> Result<(), BoxError> {
    for (slot, batch) in batches.iter_mut().enumerate() {
        if batch.is_empty() {
            continue;
        }
        let drained: Vec<(Ulid, Ulid)> = std::mem::take(batch);
        project_metadata_create_events_from_log(targets[slot].1.as_ref(), drained)
            .await
            .map_err(|error| format!("projection failed: {error:?}"))?;
    }
    Ok(())
}

// Keeps per-node pruning, but fails on a lost-progress window rather than a fixed
// budget so a slow-but-converging run is not flunked.
async fn wait_for_visibility(
    contexts: &[Arc<DriverContext>],
    pairs: &[(GroupId, Ulid)],
    poll_interval: Duration,
) -> Result<(), BoxError> {
    let mut remaining: Vec<Vec<(GroupId, Ulid)>> =
        contexts.iter().map(|_| pairs.to_vec()).collect();
    let mut best = usize::MAX;
    let mut deadline = Instant::now() + NO_PROGRESS_TIMEOUT;

    loop {
        for (context, missing) in contexts.iter().zip(remaining.iter_mut()) {
            let mut still_missing = Vec::new();
            for &(group_id, document_id) in missing.iter() {
                if drive(
                    GetMetadataDocumentOperation::new(group_id, document_id),
                    context.as_ref(),
                )
                .await
                .is_err()
                {
                    still_missing.push((group_id, document_id));
                }
            }
            *missing = still_missing;
        }
        let pending: usize = remaining.iter().map(Vec::len).sum();
        if pending == 0 {
            return Ok(());
        }
        if pending < best {
            best = pending;
            deadline = Instant::now() + NO_PROGRESS_TIMEOUT;
        }
        if Instant::now() >= deadline {
            let counts: Vec<usize> = remaining.iter().map(Vec::len).collect();
            return Err(format!("visibility stalled; missing per node: {counts:?}").into());
        }
        sleep(poll_interval).await;
    }
}

/// Waits for every sampled document to become readable on the node. Replicas
/// cover all nodes here, so each arrival is progress that resets the window.
async fn wait_sample_visible(
    context: &Arc<DriverContext>,
    pairs: &[(GroupId, Ulid)],
) -> Result<(), BoxError> {
    wait_for_convergence("sampled documents not fully visible", || async {
        let mut pending = 0;
        for &(group_id, document_id) in pairs {
            if drive(
                GetMetadataDocumentOperation::new(group_id, document_id),
                context.as_ref(),
            )
            .await
            .is_err()
            {
                pending += 1;
            }
        }
        Ok::<usize, BoxError>(pending)
    })
    .await
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
            document_sync_storage_path: Some(dir.join("document-sync")),
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

    let shutdown = aruna_core::shutdown::Shutdown::new();
    initialize_net_holder(
        context.clone(),
        aruna_core::structs::RoCrateLimits::default(),
        aruna_operations::jobs::runtime::JobsRuntime::new(),
        &shutdown,
    );
    initialize_task_incoming(
        context.clone(),
        task_handle.clone(),
        aruna_operations::jobs::runtime::JobsRuntime::new(),
    )
    .await;

    Ok(TestNode {
        _temp_dir: None,
        net,
        task_handle,
        context,
        shutdown,
    })
}

async fn respawn_with_retry(
    realm_id: RealmId,
    secret_key: iroh::SecretKey,
    dir: &Path,
) -> Result<TestNode, BoxError> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match spawn_node_with(realm_id, Some(secret_key.clone()), dir.to_path_buf()).await {
            Ok(node) => return Ok(node),
            Err(error) => {
                if Instant::now() >= deadline {
                    return Err(format!("respawn failed after retries: {error}").into());
                }
                sleep(Duration::from_millis(250)).await;
            }
        }
    }
}

async fn wire_peers(nodes: &[TestNode]) {
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

async fn install_realm_config(nodes: &[TestNode], realm_id: &RealmId) -> Result<(), BoxError> {
    let mut config = RealmConfigDocument::default_for_realm(*realm_id, Vec::new());
    config.seed_default_placement();
    for node in nodes {
        config.ensure_node(node.net.node_id(), RealmNodeKind::Management);
    }

    write_config(nodes, realm_id, &config).await?;
    for node in nodes {
        aruna_operations::process_placements::process_shard_placements(
            &node.context,
            *realm_id,
            node.net.node_id(),
        )
        .await;
    }
    Ok(())
}

async fn write_config(
    nodes: &[TestNode],
    realm_id: &RealmId,
    config: &RealmConfigDocument,
) -> Result<(), BoxError> {
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
        node.net.refresh_realm_peers_from_document(config).await?;
    }
    Ok(())
}

async fn wait_for_realm_node_convergence(
    nodes: &[TestNode],
    realm_id: &RealmId,
) -> Result<(), BoxError> {
    let expected: std::collections::HashSet<_> =
        nodes.iter().map(|node| node.net.node_id()).collect();
    wait_for_convergence("realm nodes did not converge", || async {
        let mut pending = 0;
        for node in nodes {
            match drive(
                GetRealmNodesOperation::new(*realm_id),
                node.context.as_ref(),
            )
            .await
            {
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
        let _ = node.shutdown.drain(NO_PROGRESS_TIMEOUT).await;
        let _ = node.task_handle.shutdown(NO_PROGRESS_TIMEOUT).await;
        node.net.shutdown().await;
    }
}

/// Held shard topics the incident node restored.
const INCIDENT_SHARDS: u32 = 128;
/// Only this short prefix needs revision-chain ordering during peer recovery.
const INCIDENT_METADATA_RECORDS: usize = 32;
/// The production drain examines two full topic pages per invocation.
const INCIDENT_LIMIT: usize = 2 * aruna_operations::document_sync_outbox::OUTBOX_DRAIN_BATCH_SIZE;
/// Two full invocation windows keep the scale assertion away from the boundary.
const INCIDENT_SCALE_RECORDS: usize = 2 * INCIDENT_LIMIT;
/// One bounded pass plus a short chain keeps peer-return coverage controllable.
const INCIDENT_RECORDS: usize = INCIDENT_LIMIT + INCIDENT_METADATA_RECORDS;

// With both co-holders unavailable, the scale fixture parks after one exact
// invocation cap without asking the convergence test to replay the scale data.
#[test]
fn offline_bounds_recovery() -> Result<(), BoxError> {
    let runtime = make_runtime()?;
    let result = runtime.block_on(offline_bound_body());
    runtime.shutdown_timeout(Duration::from_secs(10));
    result
}

async fn offline_bound_body() -> Result<(), BoxError> {
    use aruna_operations::startup::{RecoveryError, RecoveryOutcome};
    use aruna_operations::task_incoming::OutboxDrainer;

    assert_eq!(INCIDENT_LIMIT, 8_192);
    assert_eq!(INCIDENT_SCALE_RECORDS, 2 * INCIDENT_LIMIT);
    let realm_id = RealmId([91u8; 32]);
    let outage = prepare_outage(realm_id, INCIDENT_SCALE_RECORDS).await?;
    let (status, cancelled, driver) = spawn_recovery(&outage.live, realm_id);
    wait_degraded(&status).await?;
    let degraded = status.snapshot();
    assert!(degraded.topics_remaining > 0);
    assert!(degraded.last_progress_timestamp > 0);
    assert_eq!(degraded.last_error, Some(RecoveryError::PeerUnavailable));
    assert!(status.pass_total(RecoveryOutcome::Partial) > 0);

    let drainer = OutboxDrainer::new(drain_context(&outage.live));
    assert_drain_bound(&outage, &drainer).await?;
    stop_driver(cancelled, driver).await?;
    shutdown_nodes(vec![outage.live]).await;
    Ok(())
}

// The three-node incident path parks a bounded pass, restores both peers, and
// then proves full drain, revision order, and convergence on the same queue.
#[test]
fn offline_recovery_converges() -> Result<(), BoxError> {
    let runtime = make_runtime()?;
    let result = runtime.block_on(recovery_converges());
    runtime.shutdown_timeout(Duration::from_secs(10));
    result
}

async fn recovery_converges() -> Result<(), BoxError> {
    use aruna_operations::startup::{RecoveryError, RecoveryOutcome};
    use aruna_operations::task_incoming::OutboxDrainer;

    let realm_id = RealmId([92u8; 32]);
    let outage = prepare_outage(realm_id, INCIDENT_RECORDS).await?;
    let (status, cancelled, driver) = spawn_recovery(&outage.live, realm_id);
    wait_degraded(&status).await?;
    let degraded = status.snapshot();
    assert_eq!(degraded.last_error, Some(RecoveryError::PeerUnavailable));
    assert!(status.pass_total(RecoveryOutcome::Partial) > 0);
    let drainer = OutboxDrainer::new(drain_context(&outage.live));
    assert_drain_bound(&outage, &drainer).await?;
    finish_outage(outage, &drainer, &status, cancelled, driver).await
}

fn spawn_recovery(
    node: &TestNode,
    realm_id: RealmId,
) -> (
    aruna_operations::startup::RecoveryStatus,
    tokio_util::sync::CancellationToken,
    tokio::task::JoinHandle<()>,
) {
    use aruna_operations::startup::{RecoveryConfig, RecoveryStatus, run_recovery};
    use tokio_util::sync::CancellationToken;

    let status = RecoveryStatus::new();
    let cancelled = CancellationToken::new();
    let driver = tokio::spawn(run_recovery(
        node.context.clone(),
        RecoveryConfig {
            realm_id,
            node_id: node.net.node_id(),
            publish_full_usage: false,
        },
        status.clone(),
        cancelled.clone(),
    ));
    (status, cancelled, driver)
}

async fn prepare_outage(realm_id: RealmId, record_count: usize) -> Result<OutageFixture, BoxError> {
    use aruna_core::document::DocumentSyncTarget;

    let IncidentFixture {
        nodes,
        secrets,
        config,
        target,
        placement,
    } = incident_fixture(realm_id).await?;
    let (group_id, document_id) = match &target {
        DocumentSyncTarget::MetadataRegistry {
            group_id,
            document_id,
        } => (*group_id, *document_id),
        _ => return Err("incident target must be a metadata registry".into()),
    };
    let local = nodes[0].net.node_id();
    let holders: Vec<_> = nodes.iter().map(|node| node.net.node_id()).collect();
    let (live, dir_one, dir_two) = stop_peers(nodes).await?;
    let _ = live.task_handle.shutdown(NO_PROGRESS_TIMEOUT).await;
    let seeded = seed_outbox(
        &live,
        realm_id,
        local,
        &target,
        placement,
        &holders,
        record_count,
    )
    .await?;
    assert_eq!(seeded.len(), record_count);

    Ok(OutageFixture {
        live,
        dir_one,
        dir_two,
        secrets,
        config,
        target,
        placement,
        group_id,
        document_id,
        seeded,
    })
}

async fn assert_drain_bound(
    outage: &OutageFixture,
    drainer: &aruna_operations::task_incoming::OutboxDrainer,
) -> Result<(), BoxError> {
    let high_water = outbox_keys(&outage.live).await?;
    assert_eq!(high_water, outage.seeded);
    drainer.run_once().await;
    let (examined, cursor_parked) = drainer.rotation_progress();
    assert_eq!(examined, INCIDENT_LIMIT);
    assert!(cursor_parked, "the first invocation must park its cursor");
    assert_eq!(
        outbox_keys(&outage.live).await?,
        high_water,
        "a blocked rotation must not delete or reorder accepted work"
    );
    Ok(())
}

async fn finish_outage(
    outage: OutageFixture,
    drainer: &aruna_operations::task_incoming::OutboxDrainer,
    status: &aruna_operations::startup::RecoveryStatus,
    cancelled: tokio_util::sync::CancellationToken,
    driver: tokio::task::JoinHandle<()>,
) -> Result<(), BoxError> {
    use aruna_operations::startup::{RecoveryOutcome, RecoveryState};

    let realm_id = outage.config.realm_id;
    let nodes = restore_peers(
        realm_id,
        &outage.secrets,
        outage.dir_one,
        outage.dir_two,
        outage.live,
        &outage.config,
    )
    .await?;
    let topic = outage.target.sync_topic_id(realm_id, &outage.placement);
    assert!(
        nodes[1]
            .net
            .document_sync_topic_exists(topic)
            .unwrap_or(false),
        "restored peer must retain the genesis"
    );

    let expected_event = Ulid::from_parts(3, INCIDENT_METADATA_RECORDS as u128);
    wait_outbox(drainer, &nodes).await?;
    wait_registry(&nodes, outage.group_id, outage.document_id, expected_event).await?;
    wait_recovery(status).await?;
    assert_eq!(status.snapshot().topics_remaining, 0);
    assert_eq!(status.snapshot().state, RecoveryState::Converged);
    assert!(status.pass_total(RecoveryOutcome::Success) > 0);
    stop_driver(cancelled, driver).await?;

    shutdown_nodes(nodes).await;
    Ok(())
}

async fn stop_driver(
    cancelled: tokio_util::sync::CancellationToken,
    driver: tokio::task::JoinHandle<()>,
) -> Result<(), BoxError> {
    cancelled.cancel();
    tokio::time::timeout(NO_PROGRESS_TIMEOUT, driver)
        .await
        .map_err(|_| "recovery driver did not stop on cancellation")??;
    Ok(())
}

async fn incident_fixture(realm_id: RealmId) -> Result<IncidentFixture, BoxError> {
    let secrets = incident_secrets();
    let nodes = spawn_incident_nodes(realm_id, &secrets).await?;
    let strategy = incident_strategy();
    let config = incident_config(realm_id, &nodes, &strategy);
    write_config(&nodes, &realm_id, &config).await?;
    let (target, placement) = incident_target(&strategy)?;
    ensure_incident_topics(realm_id, &nodes, &strategy, &target, placement)?;

    Ok(IncidentFixture {
        nodes,
        secrets,
        config,
        target,
        placement,
    })
}

fn incident_secrets() -> [iroh::SecretKey; 3] {
    [
        iroh::SecretKey::from_bytes(&[0xA1; 32]),
        iroh::SecretKey::from_bytes(&[0xB1; 32]),
        iroh::SecretKey::from_bytes(&[0xB2; 32]),
    ]
}

async fn spawn_incident_nodes(
    realm_id: RealmId,
    secrets: &[iroh::SecretKey; 3],
) -> Result<Vec<TestNode>, BoxError> {
    let mut nodes = Vec::with_capacity(3);
    for secret in secrets {
        let dir = tempfile::tempdir()?;
        let mut node =
            spawn_node_with(realm_id, Some(secret.clone()), dir.path().to_path_buf()).await?;
        node._temp_dir = Some(dir);
        nodes.push(node);
    }
    wire_peers(&nodes[1..]).await;
    assert_eq!(
        nodes.len(),
        3,
        "incident fixture must have three live nodes"
    );
    Ok(nodes)
}

fn incident_strategy() -> aruna_core::structs::PlacementStrategy {
    aruna_core::structs::PlacementStrategy {
        strategy_id: Ulid::from_bytes([6u8; 16]),
        name: "incident".to_string(),
        replica_count: None,
        distinct_locations: false,
        affinity: Vec::new(),
        shard_count: INCIDENT_SHARDS,
    }
}

fn incident_config(
    realm_id: RealmId,
    nodes: &[TestNode],
    strategy: &aruna_core::structs::PlacementStrategy,
) -> RealmConfigDocument {
    use aruna_core::structs::{DocumentClass, METADATA_HANDLE, PlacementBinding, PlacementScope};
    use aruna_core::structured_id::PlacementHandle;

    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.default_strategy_id = Some(strategy.strategy_id);
    config.strategies = vec![strategy.clone()];
    config.placement_bindings = vec![PlacementBinding {
        handle: PlacementHandle::new(METADATA_HANDLE).expect("metadata handle is valid"),
        scope: PlacementScope::Realm(realm_id),
        document_class: DocumentClass::Metadata,
        strategy_id: strategy.strategy_id,
        allocator_range_id: None,
        allocated_by: None,
        allocated_at_ms: None,
    }];
    for node in nodes {
        config.ensure_node(node.net.node_id(), RealmNodeKind::Management);
    }
    config
}

fn incident_target(
    strategy: &aruna_core::structs::PlacementStrategy,
) -> Result<
    (
        aruna_core::document::DocumentSyncTarget,
        aruna_core::structs::PlacementRef,
    ),
    BoxError,
> {
    use aruna_core::MetaResourceId;
    use aruna_core::document::DocumentSyncTarget;
    use aruna_core::structured_id::{BucketId, PlacementHandle};

    let placement = aruna_core::structs::PlacementRef {
        strategy_id: strategy.strategy_id,
        epoch: 0,
        shard: 5,
    };
    let document_id: Ulid = MetaResourceId::from_parts(
        2,
        PlacementHandle::new(aruna_core::structs::METADATA_HANDLE)?,
        BucketId::new(5)?,
        2,
    )?
    .into();
    Ok((
        DocumentSyncTarget::MetadataRegistry {
            group_id: Ulid::from_parts(1, 1),
            document_id,
        },
        placement,
    ))
}

fn ensure_incident_topics(
    realm_id: RealmId,
    nodes: &[TestNode],
    strategy: &aruna_core::structs::PlacementStrategy,
    target: &aruna_core::document::DocumentSyncTarget,
    placement: aruna_core::structs::PlacementRef,
) -> Result<(), BoxError> {
    let local = nodes[0].net.node_id();
    let peer_two = nodes[2].net.node_id();
    let topics: Vec<_> = (0..INCIDENT_SHARDS)
        .map(|shard| {
            aruna_core::document::shard_topic_id(
                realm_id,
                &aruna_core::structs::PlacementRef {
                    strategy_id: strategy.strategy_id,
                    epoch: 0,
                    shard,
                },
            )
        })
        .collect();
    nodes[1]
        .net
        .ensure_document_sync_topics(&topics, vec![local, peer_two])?;
    let topic = target.sync_topic_id(realm_id, &placement);
    assert!(
        nodes[1]
            .net
            .document_sync_topic_exists(topic)
            .unwrap_or(false),
        "one peer must hold the recovery fixture genesis"
    );
    assert!(
        !nodes[0]
            .net
            .document_sync_topic_exists(topic)
            .unwrap_or(true),
        "the live node must start without the shard genesis"
    );
    Ok(())
}

async fn stop_peers(mut nodes: Vec<TestNode>) -> Result<(TestNode, TempDir, TempDir), BoxError> {
    let live = nodes.remove(0);
    let mut offline_one = nodes.remove(0);
    let mut offline_two = nodes.remove(0);
    let dir_one = offline_one
        ._temp_dir
        .take()
        .ok_or("offline peer directory missing")?;
    let dir_two = offline_two
        ._temp_dir
        .take()
        .ok_or("offline peer directory missing")?;
    let _ = offline_one.shutdown.drain(NO_PROGRESS_TIMEOUT).await;
    offline_one.net.clear_inbound_handler();
    let _ = offline_one.task_handle.shutdown(NO_PROGRESS_TIMEOUT).await;
    offline_one.net.shutdown().await;
    let _ = offline_two.shutdown.drain(NO_PROGRESS_TIMEOUT).await;
    offline_two.net.clear_inbound_handler();
    let _ = offline_two.task_handle.shutdown(NO_PROGRESS_TIMEOUT).await;
    offline_two.net.shutdown().await;
    drop(offline_one);
    drop(offline_two);
    Ok((live, dir_one, dir_two))
}

async fn seed_outbox(
    node: &TestNode,
    realm_id: RealmId,
    local: aruna_core::NodeId,
    target: &aruna_core::document::DocumentSyncTarget,
    placement: aruna_core::structs::PlacementRef,
    holders: &[aruna_core::NodeId],
    record_count: usize,
) -> Result<Vec<Vec<u8>>, BoxError> {
    for chunk_start in (0..record_count).step_by(1_024) {
        let mut writes = Vec::with_capacity(1_024);
        for index in chunk_start..(chunk_start + 1_024).min(record_count) {
            let record = incident_record(realm_id, local, target, placement, holders, index)?;
            writes.push(aruna_operations::document_sync_outbox::outbox_write_entry(
                &record,
            )?);
        }
        match node
            .context
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
            other => return Err(format!("unexpected outbox batch write: {other:?}").into()),
        }
    }
    let final_index = INCIDENT_METADATA_RECORDS
        .checked_sub(1)
        .ok_or("incident metadata chain must contain one record")?;
    write_registry(
        node,
        incident_registry(realm_id, target, placement, holders, final_index)?,
    )
    .await?;
    outbox_keys(node).await
}

fn incident_record(
    realm_id: RealmId,
    local: aruna_core::NodeId,
    target: &aruna_core::document::DocumentSyncTarget,
    placement: aruna_core::structs::PlacementRef,
    holders: &[aruna_core::NodeId],
    index: usize,
) -> Result<aruna_core::document::DocumentSyncOutboxRecord, BoxError> {
    use aruna_core::document::{
        DocumentSyncChange, DocumentSyncChangeKind, DocumentSyncOutboxEvent, DocumentSyncRevision,
    };
    if index >= INCIDENT_METADATA_RECORDS {
        return incident_delete(local, placement, holders, index);
    }
    let event_id = Ulid::from_parts(3, (index + 1) as u128);
    let base = (index > 0).then(|| DocumentSyncRevision {
        generation: index as u64,
        event_id: Ulid::from_parts(3, index as u128),
        actor: local,
        updated_at_ms: index as u64,
    });
    let change = DocumentSyncChange {
        base,
        current: DocumentSyncRevision {
            generation: (index + 1) as u64,
            event_id,
            actor: local,
            updated_at_ms: (index + 1) as u64,
        },
        kind: DocumentSyncChangeKind::Upsert,
        placement,
    };
    let registry = incident_registry(realm_id, target, placement, holders, index)?;
    Ok(
        aruna_operations::document_sync_outbox::new_outbox_record_with_id(
            Ulid::from_parts(1, index as u128),
            local,
            target.clone(),
            holders.to_vec(),
            DocumentSyncOutboxEvent::Upsert {
                bytes: postcard::to_allocvec(&registry)?,
                change,
            },
            aruna_core::structs::PlacementRef::NIL,
            false,
        ),
    )
}

fn incident_delete(
    local: aruna_core::NodeId,
    placement: aruna_core::structs::PlacementRef,
    holders: &[aruna_core::NodeId],
    index: usize,
) -> Result<aruna_core::document::DocumentSyncOutboxRecord, BoxError> {
    use aruna_core::document::{
        DocumentSyncChange, DocumentSyncChangeKind, DocumentSyncOutboxEvent, DocumentSyncRevision,
        DocumentSyncTarget,
    };
    let event_id = Ulid::from_parts(3, (index + 1) as u128);
    let change = DocumentSyncChange {
        base: None,
        current: DocumentSyncRevision {
            generation: 1,
            event_id,
            actor: local,
            updated_at_ms: (index + 1) as u64,
        },
        kind: DocumentSyncChangeKind::Delete,
        placement,
    };
    let target = DocumentSyncTarget::MetadataGraphLifecycle {
        graph_iri: format!("https://aruna.example/incident/graph/{index}"),
    };
    Ok(
        aruna_operations::document_sync_outbox::new_outbox_record_with_id(
            Ulid::from_parts(1, index as u128),
            local,
            target,
            holders.to_vec(),
            DocumentSyncOutboxEvent::Delete { change },
            aruna_core::structs::PlacementRef::NIL,
            false,
        ),
    )
}

fn incident_registry(
    realm_id: RealmId,
    target: &aruna_core::document::DocumentSyncTarget,
    placement: aruna_core::structs::PlacementRef,
    holders: &[aruna_core::NodeId],
    index: usize,
) -> Result<aruna_core::structs::MetadataRegistryRecord, BoxError> {
    use aruna_core::document::DocumentSyncTarget;
    use aruna_core::structs::MetadataRegistryRecord;

    let (group_id, document_id) = match target {
        DocumentSyncTarget::MetadataRegistry {
            group_id,
            document_id,
        } => (*group_id, *document_id),
        _ => return Err("incident target must be a metadata registry".into()),
    };
    let event_id = Ulid::from_parts(3, (index + 1) as u128);
    Ok(MetadataRegistryRecord {
        realm_id,
        group_id,
        document_id,
        document_path: "datasets/incident".to_string(),
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        public: true,
        permission_path: MetadataRegistryRecord::permission_path_for(
            &realm_id,
            group_id,
            "datasets/incident",
            document_id,
        ),
        placement,
        holder_node_ids: holders.to_vec(),
        created_at_ms: 1,
        updated_at_ms: (index + 1) as u64,
        establishing_event_id: Ulid::from_parts(3, 1),
        last_event_id: event_id,
    })
}

async fn write_registry(
    node: &TestNode,
    registry: aruna_core::structs::MetadataRegistryRecord,
) -> Result<(), BoxError> {
    let event = node
        .context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: aruna_core::keyspaces::METADATA_INDEX_KEYSPACE.to_string(),
            key: aruna_core::storage_entries::metadata_registry_key(
                registry.group_id,
                registry.document_id,
            ),
            value: postcard::to_allocvec(&registry)?.into(),
            txn_id: None,
        }))
        .await;
    match event {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        other => Err(format!("unexpected registry write event: {other:?}").into()),
    }
}

async fn wait_degraded(status: &aruna_operations::startup::RecoveryStatus) -> Result<(), BoxError> {
    use aruna_operations::startup::{RecoveryOutcome, RecoveryState};

    wait_for_convergence("recovery did not report degraded", || async {
        let snapshot = status.snapshot();
        Ok::<usize, BoxError>(usize::from(
            snapshot.state != RecoveryState::Degraded
                || status.pass_total(RecoveryOutcome::Partial) == 0,
        ))
    })
    .await
}

fn drain_context(node: &TestNode) -> Arc<DriverContext> {
    Arc::new(DriverContext {
        storage_handle: node.context.storage_handle.clone(),
        net_handle: node.context.net_handle.clone(),
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    })
}

async fn restore_peers(
    realm_id: RealmId,
    secrets: &[iroh::SecretKey; 3],
    dir_one: TempDir,
    dir_two: TempDir,
    live: TestNode,
    config: &RealmConfigDocument,
) -> Result<Vec<TestNode>, BoxError> {
    let path_one = dir_one.path().to_path_buf();
    let mut peer_one = respawn_with_retry(realm_id, secrets[1].clone(), &path_one).await?;
    peer_one._temp_dir = Some(dir_one);
    let path_two = dir_two.path().to_path_buf();
    let mut peer_two = respawn_with_retry(realm_id, secrets[2].clone(), &path_two).await?;
    peer_two._temp_dir = Some(dir_two);
    let nodes = vec![live, peer_one, peer_two];
    wire_peers(&nodes).await;
    write_config(&nodes, &realm_id, config).await?;
    Ok(nodes)
}

async fn wait_outbox(
    drainer: &aruna_operations::task_incoming::OutboxDrainer,
    nodes: &[TestNode],
) -> Result<(), BoxError> {
    // One bounded invocation exceeds five minutes under instrumentation, so
    // only a much longer interval without a shrinking key count is a stall.
    let drain = async {
        loop {
            drainer.run_once().await;
            if outbox_keys(&nodes[0]).await?.is_empty() {
                return Ok::<(), BoxError>(());
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    let watch = async {
        let mut best = usize::MAX;
        let mut deadline = Instant::now() + HANG_CAP.saturating_mul(3);
        loop {
            let pending = outbox_keys(&nodes[0]).await?.len();
            if pending < best {
                best = pending;
                deadline = Instant::now() + HANG_CAP.saturating_mul(3);
            }
            if Instant::now() >= deadline {
                return Err::<(), BoxError>(
                    format!("outbox did not drain (still pending: {pending})").into(),
                );
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    tokio::select! {
        result = drain => result,
        result = watch => result,
    }
}

async fn wait_registry(
    nodes: &[TestNode],
    group_id: GroupId,
    document_id: Ulid,
    expected_event: Ulid,
) -> Result<(), BoxError> {
    wait_for_convergence("registry did not converge", || async {
        let mut pending = 0;
        for node in nodes {
            match read_registry(node, group_id, document_id).await? {
                Some(record) if record.last_event_id == expected_event => {}
                _ => pending += 1,
            }
        }
        Ok::<usize, BoxError>(pending)
    })
    .await
}

async fn wait_recovery(status: &aruna_operations::startup::RecoveryStatus) -> Result<(), BoxError> {
    use aruna_operations::startup::RecoveryState;

    wait_for_convergence("recovery did not converge", || async {
        Ok::<usize, BoxError>(usize::from(
            status.snapshot().state != RecoveryState::Converged,
        ))
    })
    .await
}

async fn read_registry(
    node: &TestNode,
    group_id: Ulid,
    document_id: Ulid,
) -> Result<Option<aruna_core::structs::MetadataRegistryRecord>, BoxError> {
    match node
        .context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: aruna_core::keyspaces::METADATA_INDEX_KEYSPACE.to_string(),
            key: aruna_core::storage_entries::metadata_registry_key(group_id, document_id),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|bytes| postcard::from_bytes(&bytes))
            .transpose()
            .map_err(Into::into),
        other => Err(format!("unexpected registry read event: {other:?}").into()),
    }
}

async fn outbox_keys(node: &TestNode) -> Result<Vec<Vec<u8>>, BoxError> {
    let mut keys = Vec::new();
    let mut start: Option<Vec<u8>> = None;
    loop {
        let batch = aruna_operations::document_sync_outbox::read_outbox_records(
            &node.context.storage_handle,
            &[],
            start.take(),
            1_024,
        )
        .await?;
        keys.extend(batch.records.iter().map(|(key, _)| key.clone()));
        if !batch.has_more {
            return Ok(keys);
        }
        start = batch.next_start_after;
    }
}
