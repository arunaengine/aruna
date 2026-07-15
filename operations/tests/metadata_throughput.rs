use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aruna_core::NodeId;
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
    mint_local_document_id,
};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::get_metadata_document::GetMetadataDocumentOperation;
use aruna_operations::get_realm_config::GetRealmConfigOperation;
use aruna_operations::get_realm_nodes::GetRealmNodesOperation;
use aruna_operations::incoming::initialize_net_incoming;
use aruna_operations::metadata::MetadataHandle;
use aruna_operations::metadata::materialization_queue::metadata_materialization_jobs_exist;
use aruna_operations::metadata::projector::project_metadata_create_events_from_log;
use aruna_operations::task_incoming::initialize_task_incoming;
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use tempfile::TempDir;
use tokio::time::sleep;
use ulid::Ulid;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const SETUP_TIMEOUT: Duration = Duration::from_secs(30);
const PROJECTION_BATCH: usize = 32;
const TOTAL_CREATES: usize = 2000;

struct TestNode {
    _temp_dir: Option<TempDir>,
    net: NetHandle,
    task_handle: TaskHandle,
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

#[test]
#[ignore]
fn throughput_gate() -> Result<(), BoxError> {
    init_logging();

    let mut results: Vec<(usize, f64)> = Vec::new();
    for (level, &writers) in [1usize, 8, 32, 64].iter().enumerate() {
        let runtime = make_runtime()?;
        let ops = runtime.block_on(async {
            let realm_id = RealmId([91u8 + level as u8; 32]);
            let nodes = build_realm_nodes(&realm_id, 3).await?;
            let targets = node_targets(&nodes);
            let group_id = Ulid::r#gen();

            let per_writer = TOTAL_CREATES / writers;
            let label = format!("tp{writers}");
            let started = Instant::now();
            let mut handles = Vec::with_capacity(writers);
            for writer in 0..writers {
                let targets = targets.clone();
                let label = label.clone();
                handles.push(tokio::spawn(async move {
                    run_writer(realm_id, group_id, &label, writer, per_writer, targets).await
                }));
            }
            let mut total = 0usize;
            for handle in handles {
                total += handle.await??.len();
            }
            let elapsed = started.elapsed().as_secs_f64();
            let ops = total as f64 / elapsed;
            println!("writers={writers} total={total} elapsed={elapsed:.3} ops_per_sec={ops:.1}");
            shutdown_nodes(nodes).await;
            Ok::<f64, BoxError>(ops)
        })?;
        runtime.shutdown_timeout(Duration::from_secs(10));
        results.push((writers, ops));
    }

    let best = results.iter().map(|(_, ops)| *ops).fold(0.0f64, f64::max);
    let ops_1 = results
        .iter()
        .find(|(w, _)| *w == 1)
        .map(|(_, o)| *o)
        .unwrap();
    let ops_8 = results
        .iter()
        .find(|(w, _)| *w == 8)
        .map(|(_, o)| *o)
        .unwrap();
    assert!(
        best >= 1000.0,
        "throughput gate failed: best={best:.1} ops/s < 1000 ops/s ({results:?})"
    );
    assert!(
        ops_8 > ops_1,
        "scaling gate failed: 8 writers ({ops_8:.1} ops/s) <= 1 writer ({ops_1:.1} ops/s)"
    );
    Ok(())
}

#[test]
#[ignore]
fn convergence_gate() -> Result<(), BoxError> {
    init_logging();
    let runtime = make_runtime()?;
    let seconds = runtime.block_on(async {
        let realm_id = RealmId([122u8; 32]);
        let nodes = build_realm_nodes(&realm_id, 3).await?;
        let targets = node_targets(&nodes);
        let group_id = Ulid::r#gen();

        let writers = 64usize;
        let per_writer = 16usize;
        let mut handles = Vec::with_capacity(writers);
        for writer in 0..writers {
            let targets = targets.clone();
            handles.push(tokio::spawn(async move {
                run_writer(realm_id, group_id, "conv", writer, per_writer, targets).await
            }));
        }
        let mut created: Vec<(GroupId, Ulid, Instant)> = Vec::new();
        for handle in handles {
            created.extend(handle.await??);
        }
        let t0 = Instant::now();
        println!(
            "created={} docs, polling for convergence of last 100",
            created.len()
        );

        created.sort_by_key(|(_, _, at)| *at);
        let last: Vec<(GroupId, Ulid)> = created
            .iter()
            .rev()
            .take(100)
            .map(|(group_id, document_id, _)| (*group_id, *document_id))
            .collect();

        let contexts: Vec<Arc<DriverContext>> = nodes.iter().map(|n| n.context.clone()).collect();
        let result = wait_for_visibility(
            &contexts,
            &last,
            Duration::from_millis(200),
            Duration::from_secs(60),
            t0,
        )
        .await;
        shutdown_nodes(nodes).await;
        result
    })?;
    runtime.shutdown_timeout(Duration::from_secs(10));
    println!("convergence_seconds={seconds:.3}");
    assert!(
        seconds <= 15.0,
        "convergence gate failed: {seconds:.3}s > 15s"
    );
    Ok(())
}

// Exercises the production trigger chain end to end: create operation +
// projection wake (same call the API create handler debounces into), then the
// outbox drain timer, irokle publish/fan-out, and peer-side reconcile +
// projection + materialization. Converged means every node's registry holds
// every document and no materialization jobs remain anywhere.
#[test]
#[ignore]
fn production_path_convergence_gate() -> Result<(), BoxError> {
    init_logging();
    let runtime = make_runtime()?;
    let (seconds, total) = runtime.block_on(async {
        let realm_id = RealmId([124u8; 32]);
        let nodes = build_realm_nodes(&realm_id, 3).await?;
        let targets = node_targets(&nodes);
        let group_id = Ulid::r#gen();

        let writers = 32usize;
        let per_writer = 128usize;
        let started = Instant::now();
        let mut handles = Vec::with_capacity(writers);
        for writer in 0..writers {
            let targets = targets.clone();
            handles.push(tokio::spawn(async move {
                run_writer(realm_id, group_id, "prod", writer, per_writer, targets).await
            }));
        }
        let mut created: Vec<(GroupId, Ulid, Instant)> = Vec::new();
        for handle in handles {
            created.extend(handle.await??);
        }
        let total = created.len();
        println!(
            "created={total} docs in {:.3}s, polling for full cluster convergence",
            started.elapsed().as_secs_f64()
        );

        let pairs: Vec<(GroupId, Ulid)> = created
            .iter()
            .map(|(group_id, document_id, _)| (*group_id, *document_id))
            .collect();
        let contexts: Vec<Arc<DriverContext>> = nodes.iter().map(|n| n.context.clone()).collect();
        wait_for_visibility(
            &contexts,
            &pairs,
            Duration::from_millis(200),
            Duration::from_secs(300),
            started,
        )
        .await?;
        wait_for_empty_materialization_queues(&contexts, Duration::from_secs(300), started).await?;
        let seconds = started.elapsed().as_secs_f64();
        shutdown_nodes(nodes).await;
        Ok::<(f64, usize), BoxError>((seconds, total))
    })?;
    runtime.shutdown_timeout(Duration::from_secs(10));
    let rate = total as f64 / seconds;
    println!(
        "production_path_convergence_seconds={seconds:.3} docs={total} drain_docs_per_sec={rate:.1}"
    );
    assert!(
        seconds <= 15.0,
        "production path gate failed: {seconds:.3}s > 15s ({rate:.1} docs/s)"
    );
    Ok(())
}

async fn wait_for_empty_materialization_queues(
    contexts: &[Arc<DriverContext>],
    timeout: Duration,
    t0: Instant,
) -> Result<(), BoxError> {
    loop {
        let mut busy = 0usize;
        for context in contexts {
            if metadata_materialization_jobs_exist(&context.storage_handle)
                .await
                .map_err(|error| format!("materialization probe failed: {error:?}"))?
            {
                busy += 1;
            }
        }
        if busy == 0 {
            return Ok(());
        }
        if t0.elapsed() > timeout {
            return Err(format!(
                "materialization queues still busy on {busy} nodes after {timeout:?}"
            )
            .into());
        }
        sleep(Duration::from_millis(200)).await;
    }
}

#[test]
#[ignore]
fn churn_convergence_gate() -> Result<(), BoxError> {
    init_logging();
    let runtime = make_runtime()?;
    let seconds = runtime.block_on(churn_convergence_body())?;
    runtime.shutdown_timeout(Duration::from_secs(10));
    println!("catchup_seconds={seconds:.3}");
    assert!(seconds <= 15.0, "churn gate failed: {seconds:.3}s > 15s");
    Ok(())
}

async fn churn_convergence_body() -> Result<f64, BoxError> {
    let realm_id = RealmId([123u8; 32]);
    let node2_dir = tempfile::tempdir()?;
    let secret = iroh::SecretKey::from_bytes(&[7u8; 32]);

    let aux = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;

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

    let group_id = Ulid::r#gen();
    let targets0 = vec![(nodes[0].net.node_id(), nodes[0].context.clone())];
    let initial = run_writer(realm_id, group_id, "seed", 0, 1, targets0.clone()).await?;
    let initial_pair = vec![(initial[0].0, initial[0].1)];
    {
        let contexts: Vec<Arc<DriverContext>> = nodes.iter().map(|n| n.context.clone()).collect();
        wait_for_visibility(
            &contexts,
            &initial_pair,
            Duration::from_millis(200),
            SETUP_TIMEOUT,
            Instant::now(),
        )
        .await?;
    }
    println!("initial doc converged to all 3 nodes");

    let node2 = nodes.pop().expect("node 2 present");
    node2.net.clear_inbound_handler();
    node2.task_handle.clear_inbound_handler().await;
    node2.net.shutdown().await;
    drop(node2);
    tokio::task::spawn_blocking(move || aux.shutdown_timeout(Duration::from_secs(10))).await?;
    println!("node 2 shut down");

    let created = run_writer(realm_id, group_id, "churn", 0, 200, targets0).await?;
    let pairs: Vec<(GroupId, Ulid)> = created.iter().map(|(g, d, _)| (*g, *d)).collect();
    println!("created {} docs while node 2 was down", pairs.len());

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
    let t0 = Instant::now();
    println!("node 2 respawned, polling for catch-up");

    let result = wait_for_visibility(
        std::slice::from_ref(&node2.context),
        &pairs,
        Duration::from_millis(500),
        Duration::from_secs(60),
        t0,
    )
    .await;
    nodes.push(node2);
    shutdown_nodes(nodes).await;
    result
}

fn node_targets(nodes: &[TestNode]) -> Vec<(NodeId, Arc<DriverContext>)> {
    nodes
        .iter()
        .map(|node| (node.net.node_id(), node.context.clone()))
        .collect()
}

fn scaffold_payload(label: &str, writer: usize, index: usize) -> CreateMetadataDocumentPayload {
    CreateMetadataDocumentPayload::Scaffold {
        name: format!("Bench Dataset {label}-{writer}-{index}"),
        description: "Throughput benchmark document".to_string(),
        date_published: "2026-06-10".to_string(),
        license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
    }
}

fn rocrate_payload(document_id: MetaResourceId) -> CreateMetadataDocumentPayload {
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
      "description": "Throughput benchmark crate",
      "datePublished": "2026-06-10",
      "license": {{"@id": "https://creativecommons.org/licenses/by/4.0/"}}
    }}
  ]
}}"#
    );
    CreateMetadataDocumentPayload::RoCrate { jsonld }
}

async fn run_writer(
    realm_id: RealmId,
    group_id: GroupId,
    label: &str,
    writer: usize,
    count: usize,
    targets: Vec<(NodeId, Arc<DriverContext>)>,
) -> Result<Vec<(GroupId, Ulid, Instant)>, BoxError> {
    let mut batches: Vec<Vec<(Ulid, Ulid)>> = targets.iter().map(|_| Vec::new()).collect();
    let mut pending = 0usize;
    let mut created = Vec::with_capacity(count);

    let config = drive(
        GetRealmConfigOperation::new(realm_id),
        targets[0].1.as_ref(),
    )
    .await
    .map_err(|error| format!("realm config load failed: {error:?}"))?;

    for index in 0..count {
        let slot = (writer + index) % targets.len();
        let (node_id, context) = &targets[slot];
        let actor = Actor {
            node_id: *node_id,
            user_id: UserId::local(Ulid::r#gen(), realm_id),
            realm_id,
        };
        let document_path = format!("datasets/bench-{label}-{writer}-{index}");
        let document_id = mint_local_document_id(&config, &actor, group_id, &document_path)
            .map_err(|error| format!("mint failed writer={writer} index={index}: {error:?}"))?;
        let payload = if index % 2 == 0 {
            scaffold_payload(label, writer, index)
        } else {
            rocrate_payload(document_id)
        };
        let result = drive(
            CreateMetadataDocumentOperation::new_for_generated_document_id(
                CreateMetadataDocumentConfig {
                    actor,
                    group_id,
                    document_id,
                    document_path,
                    public: true,
                    payload,
                },
            ),
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
    t0: Instant,
) -> Result<f64, BoxError> {
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
            return Ok(t0.elapsed().as_secs_f64());
        }
        if t0.elapsed() > timeout {
            let counts: Vec<usize> = remaining.iter().map(Vec::len).collect();
            return Err(format!(
                "visibility timeout after {timeout:?}; missing per node: {counts:?}"
            )
            .into());
        }
        sleep(poll_interval).await;
    }
}

async fn build_realm_nodes(realm_id: &RealmId, count: usize) -> Result<Vec<TestNode>, BoxError> {
    let mut nodes = Vec::with_capacity(count);
    for _ in 0..count {
        nodes.push(spawn_node(*realm_id).await?);
    }
    wire_peers(&nodes).await;

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
    install_realm_config(&nodes, realm_id).await?;
    Ok(nodes)
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
    initialize_task_incoming(context.clone(), task_handle.clone()).await;

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
    // Config apply hook: the shard's rank-0 holder eagerly creates each
    // shard topic genesis (mirrors the production realm-config apply path).
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
    let expected: HashSet<_> = nodes.iter().map(|node| node.net.node_id()).collect();
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
