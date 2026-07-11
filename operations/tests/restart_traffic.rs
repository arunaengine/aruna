use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

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
};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::get_metadata_document::GetMetadataDocumentOperation;
use aruna_operations::get_realm_nodes::GetRealmNodesOperation;
use aruna_operations::incoming::initialize_net_incoming;
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

// Realm-node and document convergence poll to a condition; the ceilings only
// bound a genuine hang. Convergence measures single-digit seconds, but a
// loaded CI runner can stall consecutive peer syncs for the full 30s peer-sync
// timeout each, so the backstop is 2-3x that timeout, not the expected latency.
const SETUP_TIMEOUT: Duration = Duration::from_secs(120);
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(120);
const PROJECTION_BATCH: usize = 32;
const SEED_DOCUMENTS: usize = 500;

struct TestNode {
    _temp_dir: Option<TempDir>,
    net: NetHandle,
    task_handle: TaskHandle,
    context: Arc<DriverContext>,
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
    wait_for_realm_node_convergence(&nodes, &realm_id).await?;
    install_realm_config(&nodes, &realm_id).await?;

    // Seed ~500 documents across shards from node 0.
    let group_id = Ulid::r#gen();
    let targets0 = vec![(nodes[0].net.node_id(), nodes[0].context.clone())];
    let created = run_writer(realm_id, group_id, SEED_DOCUMENTS, targets0).await?;
    // Wait until the restarting node holds real shard state (a recent sample is
    // visible), so the restore has something it could wrongly re-announce.
    let sample: Vec<(GroupId, Ulid)> = created
        .iter()
        .rev()
        .take(40)
        .map(|(group_id, document_id)| (*group_id, *document_id))
        .collect();
    wait_for_any_visibility(
        &nodes[2].context,
        &sample,
        Duration::from_millis(200),
        CONVERGENCE_TIMEOUT,
    )
    .await?;
    println!(
        "seeded {} docs, node 2 holds replicated state",
        created.len()
    );

    // Restart node 2's whole stack.
    let node2 = nodes.pop().expect("node 2 present");
    node2.net.clear_inbound_handler();
    node2.task_handle.clear_inbound_handler().await;
    node2.net.shutdown().await;
    drop(node2);
    aux.shutdown().await;
    println!("node 2 shut down");

    let node2 = respawn_with_retry(realm_id, secret, node2_dir.path()).await?;
    for other in &nodes {
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

    // (a) The startup restore touches O(held shards) topics, not O(documents).
    let summary = restore_shard_subscriptions(&node2.context, node2.net.node_id(), realm_id).await;
    println!(
        "restore summary: held_shards={} shard_topics={} shared_topics={} total={}",
        summary.held_shards,
        summary.shard_topics,
        summary.shared_topics,
        summary.total_topics()
    );
    assert!(summary.held_shards > 0, "node 2 must hold shards");
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

    // (b) A fresh write after the restart converges to all nodes.
    nodes.push(node2);
    let fresh = run_writer(realm_id, group_id, 1, {
        vec![(nodes[0].net.node_id(), nodes[0].context.clone())]
    })
    .await?;
    let contexts: Vec<Arc<DriverContext>> = nodes.iter().map(|node| node.context.clone()).collect();
    wait_for_visibility(
        &contexts,
        &fresh,
        Duration::from_millis(200),
        CONVERGENCE_TIMEOUT,
    )
    .await?;
    println!("fresh write converged to all nodes after restart");

    shutdown_nodes(nodes).await;
    Ok(())
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

    async fn shutdown(mut self) {
        if let Some(runtime) = self.0.take() {
            let _ = tokio::task::spawn_blocking(move || {
                runtime.shutdown_timeout(Duration::from_secs(10))
            })
            .await;
        }
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
    targets: Vec<(aruna_core::NodeId, Arc<DriverContext>)>,
) -> Result<Vec<(GroupId, Ulid)>, BoxError> {
    let mut batches: Vec<Vec<(Ulid, Ulid)>> = targets.iter().map(|_| Vec::new()).collect();
    let mut pending = 0usize;
    let mut created = Vec::with_capacity(count);

    for index in 0..count {
        let slot = index % targets.len();
        let (node_id, context) = &targets[slot];
        let document_id = Ulid::r#gen();
        let result = drive(
            CreateMetadataDocumentOperation::new_for_generated_document_id(
                CreateMetadataDocumentConfig {
                    actor: Actor {
                        node_id: *node_id,
                        user_id: UserId::local(Ulid::r#gen(), realm_id),
                        realm_id,
                    },
                    group_id,
                    document_id,
                    document_path: format!("datasets/restart-{index}"),
                    public: true,
                    payload: CreateMetadataDocumentPayload::Scaffold {
                        name: format!("Restart Dataset {index}"),
                        description: "Restart traffic document".to_string(),
                        date_published: "2026-07-07".to_string(),
                        license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
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

async fn wait_for_visibility(
    contexts: &[Arc<DriverContext>],
    pairs: &[(GroupId, Ulid)],
    poll_interval: Duration,
    timeout: Duration,
) -> Result<(), BoxError> {
    let t0 = Instant::now();
    let mut remaining: Vec<Vec<(GroupId, Ulid)>> =
        contexts.iter().map(|_| pairs.to_vec()).collect();

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
        if remaining.iter().all(Vec::is_empty) {
            return Ok(());
        }
        if t0.elapsed() > timeout {
            let counts: Vec<usize> = remaining.iter().map(Vec::len).collect();
            return Err(format!("visibility timeout; missing per node: {counts:?}").into());
        }
        sleep(poll_interval).await;
    }
}

async fn wait_for_any_visibility(
    context: &Arc<DriverContext>,
    pairs: &[(GroupId, Ulid)],
    poll_interval: Duration,
    timeout: Duration,
) -> Result<(), BoxError> {
    let t0 = Instant::now();
    loop {
        for &(group_id, document_id) in pairs {
            if drive(
                GetMetadataDocumentOperation::new(group_id, document_id),
                context.as_ref(),
            )
            .await
            .is_ok()
            {
                return Ok(());
            }
        }
        if t0.elapsed() > timeout {
            return Err("visibility timeout; no sampled documents visible".into());
        }
        sleep(poll_interval).await;
    }
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
    });

    initialize_net_incoming(context.clone());
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
    Ok(())
}

async fn wait_for_realm_node_convergence(
    nodes: &[TestNode],
    realm_id: &RealmId,
) -> Result<(), BoxError> {
    let expected: std::collections::HashSet<_> =
        nodes.iter().map(|node| node.net.node_id()).collect();
    let deadline = Instant::now() + SETUP_TIMEOUT;

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
