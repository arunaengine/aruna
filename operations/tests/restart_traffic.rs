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

mod convergence;
use convergence::{HANG_CAP, NO_PROGRESS_TIMEOUT, wait_for_convergence};

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
    let group_id = Ulid::generate();
    let targets0 = vec![(nodes[0].net.node_id(), nodes[0].context.clone())];
    let created = run_writer(realm_id, group_id, SEED_DOCUMENTS, "seed", targets0).await?;
    // Wait until the restarting node holds real shard state (a recent sample is
    // visible), so the restore has something it could wrongly re-announce.
    let sample: Vec<(GroupId, Ulid)> = created
        .iter()
        .rev()
        .take(40)
        .map(|(group_id, document_id)| (*group_id, *document_id))
        .collect();
    wait_for_any_visibility(&nodes[2].context, &sample).await?;
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
    aux.shutdown().await?;
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

async fn wait_for_any_visibility(
    context: &Arc<DriverContext>,
    pairs: &[(GroupId, Ulid)],
) -> Result<(), BoxError> {
    wait_for_convergence(
        "visibility timeout; no sampled documents visible",
        || async {
            for &(group_id, document_id) in pairs {
                if drive(
                    GetMetadataDocumentOperation::new(group_id, document_id),
                    context.as_ref(),
                )
                .await
                .is_ok()
                {
                    return Ok(0);
                }
            }
            Ok(1)
        },
    )
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
        node.net.shutdown().await;
    }
}

/// Held shard topics the incident node restored.
const INCIDENT_SHARDS: u32 = 128;
/// Outbox records seeded at the metric scan ceiling observed in the incident.
const INCIDENT_RECORDS: usize = 8_192;

// A node holding 128 shard topics whose co-holders are all unavailable, with the
// outbox at its scan ceiling: recovery stays bounded and degraded, and neither
// the drain nor a restart's cursor reset loses or reorders a record.
#[test]
fn offline_bounds_recovery() -> Result<(), BoxError> {
    let runtime = make_runtime()?;
    let result = runtime.block_on(offline_recovery_body());
    runtime.shutdown_timeout(Duration::from_secs(10));
    result
}

async fn offline_recovery_body() -> Result<(), BoxError> {
    use aruna_core::document::{
        DocumentSyncChange, DocumentSyncChangeKind, DocumentSyncOutboxEvent, DocumentSyncRevision,
        DocumentSyncTarget,
    };
    use aruna_core::structs::{PlacementRef, PlacementStrategy};
    use aruna_operations::startup::{
        RecoveryConfig, RecoveryState, RecoveryStatus, SHARD_RESTORE_UNIT_BUDGET,
        ShardRestoreCursor, restore_shard_pass, run_recovery,
    };
    use aruna_operations::task_incoming::OutboxDrainer;
    use tokio_util::sync::CancellationToken;

    let realm_id = RealmId([91u8; 32]);
    let node = spawn_node(realm_id).await?;
    let local = node.net.node_id();
    // Never spawned and never dialled: config-only peers are the fixture.
    let offline = [
        iroh::SecretKey::from_bytes(&[0xB1; 32]).public(),
        iroh::SecretKey::from_bytes(&[0xB2; 32]).public(),
    ];

    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    let strategy = PlacementStrategy {
        strategy_id: Ulid::from_bytes([6u8; 16]),
        name: "incident".to_string(),
        replica_count: None,
        distinct_locations: false,
        affinity: Vec::new(),
        shard_count: INCIDENT_SHARDS,
    };
    config.default_strategy_id = Some(strategy.strategy_id);
    config.strategies = vec![strategy.clone()];
    config.ensure_node(local, RealmNodeKind::Management);
    for peer in offline {
        config.ensure_node(peer, RealmNodeKind::Management);
    }
    let actor = Actor {
        node_id: local,
        user_id: UserId::nil(realm_id),
        realm_id,
    };
    match node
        .context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: (*realm_id.as_bytes()).into(),
            value: config.to_bytes(&actor)?.into(),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => return Err(format!("unexpected realm config write event: {other:?}").into()),
    }

    // A bounded pass never exceeds its work-unit limit, so a later group cannot
    // be starved by an unavailable one.
    let mut cursor = ShardRestoreCursor::default();
    let pass = restore_shard_pass(
        &node.context,
        local,
        realm_id,
        &mut cursor,
        &CancellationToken::new(),
    )
    .await;
    assert!(pass.summary.held_shards > 0, "the node must hold shards");
    assert!(
        pass.units_processed <= SHARD_RESTORE_UNIT_BUDGET,
        "one pass processed {} units, over its {SHARD_RESTORE_UNIT_BUDGET} budget",
        pass.units_processed
    );
    if !pass.wrapped {
        assert_ne!(cursor, ShardRestoreCursor::default(), "cursor must advance");
    }

    // All records target one shard topic whose genesis this node cannot mint
    // while its co-holders are unavailable.
    let change = DocumentSyncChange {
        base: None,
        current: DocumentSyncRevision {
            generation: 1,
            event_id: Ulid::from_parts(3, 1),
            actor: local,
            updated_at_ms: 1,
        },
        kind: DocumentSyncChangeKind::Upsert,
        placement: PlacementRef {
            strategy_id: strategy.strategy_id,
            epoch: 0,
            shard: 5,
        },
    };
    let target = DocumentSyncTarget::MetadataRegistry {
        group_id: Ulid::from_parts(1, 1),
        document_id: Ulid::from_parts(2, 2),
    };
    for chunk_start in (0..INCIDENT_RECORDS).step_by(1_024) {
        let mut writes = Vec::with_capacity(1_024);
        for index in chunk_start..(chunk_start + 1_024).min(INCIDENT_RECORDS) {
            let record = aruna_operations::document_sync_outbox::new_outbox_record_with_id(
                Ulid::from_parts(1, index as u128),
                local,
                target.clone(),
                Vec::new(),
                DocumentSyncOutboxEvent::Upsert {
                    bytes: Vec::new(),
                    change,
                },
                PlacementRef::NIL,
                false,
            );
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
    let seeded = outbox_keys(&node).await?;
    assert_eq!(seeded.len(), INCIDENT_RECORDS);

    // A bounded invocation parks the rotation mid-keyspace and deletes nothing
    // while the topic is blocked.
    let drainer = OutboxDrainer::new(node.context.clone());
    drainer.run_once().await;
    let (examined, cursor_parked) = drainer.rotation_progress();
    assert!(
        examined <= INCIDENT_RECORDS,
        "one invocation examined {examined} records"
    );
    assert!(
        examined < INCIDENT_RECORDS || !cursor_parked,
        "an invocation that examined everything must have closed its rotation"
    );
    assert_eq!(
        outbox_keys(&node).await?,
        seeded,
        "a blocked rotation must not delete or reorder accepted work"
    );

    // Continuations stay bounded and the cursor survives every yield.
    for _ in 0..3 {
        drainer.run_once().await;
    }
    assert_eq!(
        outbox_keys(&node).await?,
        seeded,
        "continuations must not delete or reorder accepted work"
    );

    // A fresh drainer is the cursor reset a process restart performs.
    let restarted = OutboxDrainer::new(node.context.clone());
    restarted.run_once().await;
    assert_eq!(
        outbox_keys(&node).await?,
        seeded,
        "the restart cursor reset must not change the outbox"
    );

    // The driver reports degraded, not a start failure, and stops on cancel.
    let status = RecoveryStatus::new();
    let cancelled = CancellationToken::new();
    let driver = tokio::spawn(run_recovery(
        node.context.clone(),
        RecoveryConfig {
            realm_id,
            node_id: local,
            publish_full_usage: false,
        },
        status.clone(),
        cancelled.clone(),
    ));
    let deadline = Instant::now() + HANG_CAP;
    while status.snapshot().state != RecoveryState::Degraded {
        if Instant::now() >= deadline {
            cancelled.cancel();
            let _ = driver.await;
            return Err("recovery never reported degraded with peers unavailable".into());
        }
        sleep(Duration::from_millis(50)).await;
    }
    let snapshot = status.snapshot();
    assert!(snapshot.topics_remaining > 0);
    assert!(snapshot.last_progress_timestamp > 0);
    cancelled.cancel();
    tokio::time::timeout(NO_PROGRESS_TIMEOUT, driver)
        .await
        .map_err(|_| "recovery driver did not stop on cancellation")??;

    node.net.shutdown().await;
    Ok(())
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
