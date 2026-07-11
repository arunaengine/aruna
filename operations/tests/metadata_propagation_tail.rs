use std::collections::HashSet;
use std::path::PathBuf;
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
};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::get_metadata_document::GetMetadataDocumentOperation;
use aruna_operations::get_realm_nodes::GetRealmNodesOperation;
use aruna_operations::incoming::initialize_net_incoming;
use aruna_operations::metadata::MetadataHandle;
use aruna_operations::metadata::materialization_queue::metadata_materialization_jobs_exist;
use aruna_operations::metadata::projector::project_metadata_create_events_from_log;
use aruna_operations::task_incoming::initialize_task_incoming;
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use tempfile::TempDir;
use tokio::time::{MissedTickBehavior, sleep};
use ulid::Ulid;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const SETUP_TIMEOUT: Duration = Duration::from_secs(30);
const PROJECTION_BATCH: usize = 32;

const WRITERS: usize = 24;
const DOCS_PER_WRITER: usize = 500;
const WRITER_PERIOD: Duration = Duration::from_millis(40);
const PROBE_PERIOD: Duration = Duration::from_millis(500);
const PROBE_POLL: Duration = Duration::from_millis(50);
const PROBE_TIMEOUT: Duration = Duration::from_secs(60);
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(300);
const P95_TARGET_MS: u128 = 5000;

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

// Reproduces the per-document propagation tail seen in the 3-node cluster:
// sustained paced ingest (~600 creates/s for ~20s) while a sampler measures,
// for one probe document every 500ms, the time until that document is visible
// on all 3 nodes.
#[test]
#[ignore] // timing gate: run in release, like the metadata_throughput gates
fn propagation_tail_under_sustained_load() -> Result<(), BoxError> {
    init_logging();
    let runtime = make_runtime()?;
    let outcome = runtime.block_on(async {
        let realm_id = RealmId([125u8; 32]);
        let nodes = build_realm_nodes(&realm_id, 3).await?;
        let targets = node_targets(&nodes);
        let contexts: Vec<Arc<DriverContext>> = nodes.iter().map(|n| n.context.clone()).collect();
        let group_id = Ulid::r#gen();

        let (done_tx, done_rx) = tokio::sync::watch::channel(false);
        let sampler = tokio::spawn(run_sampler(
            realm_id,
            group_id,
            targets.clone(),
            contexts.clone(),
            done_rx,
        ));

        let started = Instant::now();
        let mut handles = Vec::with_capacity(WRITERS);
        for writer in 0..WRITERS {
            let targets = targets.clone();
            handles.push(tokio::spawn(async move {
                run_paced_writer(realm_id, group_id, "tail", writer, DOCS_PER_WRITER, targets)
                    .await
            }));
        }
        let mut created: Vec<(GroupId, Ulid, Instant)> = Vec::new();
        for handle in handles {
            created.extend(handle.await??);
        }
        let ingest_seconds = started.elapsed().as_secs_f64();
        let total = created.len();
        let ingest_rate = total as f64 / ingest_seconds;
        let _ = done_tx.send(true);
        println!("ingest done: docs={total} ingest_seconds={ingest_seconds:.3} ingest_docs_per_sec={ingest_rate:.1}");

        let pairs: Vec<(GroupId, Ulid)> = created
            .iter()
            .map(|(group_id, document_id, _)| (*group_id, *document_id))
            .collect();
        let convergence = async {
            wait_for_visibility(
                &contexts,
                &pairs,
                Duration::from_millis(200),
                CONVERGENCE_TIMEOUT,
                started,
            )
            .await?;
            wait_for_empty_materialization_queues(&contexts, CONVERGENCE_TIMEOUT, started).await?;
            Ok::<f64, BoxError>(started.elapsed().as_secs_f64())
        };
        let (sampler_result, convergence_result) = tokio::join!(sampler, convergence);
        let (mut latencies, failures) = sampler_result??;
        let convergence_seconds = convergence_result?;

        shutdown_nodes(nodes).await;
        latencies.sort_unstable();
        Ok::<_, BoxError>((
            latencies,
            failures,
            total,
            ingest_seconds,
            ingest_rate,
            convergence_seconds,
        ))
    })?;
    runtime.shutdown_timeout(Duration::from_secs(10));

    let (latencies, failures, total, ingest_seconds, ingest_rate, convergence_seconds) = outcome;
    let p50 = percentile(&latencies, 50.0);
    let p90 = percentile(&latencies, 90.0);
    let p95 = percentile(&latencies, 95.0);
    let max = latencies.last().copied().unwrap_or(0);
    println!(
        "probes={} probe_failures={failures} probe_p50_ms={p50} probe_p90_ms={p90} probe_p95_ms={p95} probe_max_ms={max}",
        latencies.len() + failures
    );
    println!(
        "docs={total} ingest_seconds={ingest_seconds:.3} ingest_docs_per_sec={ingest_rate:.1} convergence_seconds={convergence_seconds:.3}"
    );

    assert_eq!(
        failures, 0,
        "{failures} probes did not become visible on all nodes within {PROBE_TIMEOUT:?}"
    );
    if p95 > P95_TARGET_MS {
        println!("TAIL TARGET VIOLATED locally: p95={p95}ms (target <= {P95_TARGET_MS}ms)");
    }
    assert!(
        p95 <= P95_TARGET_MS,
        "TAIL TARGET VIOLATED locally: p95={p95}ms (target <= {P95_TARGET_MS}ms)"
    );
    Ok(())
}

fn percentile(sorted: &[u128], pct: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = ((sorted.len() as f64) * pct / 100.0).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

async fn run_sampler(
    realm_id: RealmId,
    group_id: GroupId,
    targets: Vec<(NodeId, Arc<DriverContext>)>,
    contexts: Vec<Arc<DriverContext>>,
    mut ingest_done: tokio::sync::watch::Receiver<bool>,
) -> Result<(Vec<u128>, usize), BoxError> {
    let mut ticker = tokio::time::interval(PROBE_PERIOD);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut index = 0usize;
    let mut waiters = Vec::new();
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = ingest_done.changed() => break,
        }
        if *ingest_done.borrow() {
            break;
        }
        let slot = index % targets.len();
        let (node_id, context) = targets[slot].clone();
        let document_id = Ulid::r#gen();
        let t0 = Instant::now();
        let result = drive(
            CreateMetadataDocumentOperation::new_for_generated_document_id(
                CreateMetadataDocumentConfig {
                    actor: Actor {
                        node_id,
                        user_id: UserId::local(Ulid::r#gen(), realm_id),
                        realm_id,
                    },
                    group_id,
                    document_id,
                    document_path: format!("probe/tail-{index}"),
                    public: true,
                    payload: scaffold_payload("probe", 0, index),
                },
            ),
            context.as_ref(),
        )
        .await
        .map_err(|error| format!("probe create failed index={index}: {error:?}"))?;
        project_metadata_create_events_from_log(
            context.as_ref(),
            vec![(result.record.document_id, result.record.last_event_id)],
        )
        .await
        .map_err(|error| format!("probe projection failed index={index}: {error:?}"))?;

        let probe_id = result.record.document_id;
        let contexts = contexts.clone();
        waiters.push(tokio::spawn(async move {
            wait_until_visible_on_all(&contexts, group_id, probe_id, t0).await
        }));
        index += 1;
    }

    let mut latencies = Vec::with_capacity(waiters.len());
    let mut failures = 0usize;
    for waiter in waiters {
        match waiter.await? {
            Some(elapsed) => latencies.push(elapsed.as_millis()),
            None => failures += 1,
        }
    }
    Ok((latencies, failures))
}

async fn wait_until_visible_on_all(
    contexts: &[Arc<DriverContext>],
    group_id: GroupId,
    document_id: Ulid,
    t0: Instant,
) -> Option<Duration> {
    let mut pending: Vec<Arc<DriverContext>> = contexts.to_vec();
    loop {
        let mut still_missing = Vec::with_capacity(pending.len());
        for context in pending {
            if drive(
                GetMetadataDocumentOperation::new(group_id, document_id),
                context.as_ref(),
            )
            .await
            .is_err()
            {
                still_missing.push(context);
            }
        }
        pending = still_missing;
        if pending.is_empty() {
            return Some(t0.elapsed());
        }
        if t0.elapsed() > PROBE_TIMEOUT {
            return None;
        }
        sleep(PROBE_POLL).await;
    }
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
        description: "Propagation tail benchmark document".to_string(),
        date_published: "2026-06-10".to_string(),
        license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
    }
}

fn rocrate_payload(document_id: Ulid) -> CreateMetadataDocumentPayload {
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
    CreateMetadataDocumentPayload::RoCrate { jsonld }
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

    for index in 0..count {
        ticker.tick().await;
        let slot = (writer + index) % targets.len();
        let (node_id, context) = &targets[slot];
        let document_id = Ulid::r#gen();
        let payload = if index % 2 == 0 {
            scaffold_payload(label, writer, index)
        } else {
            rocrate_payload(document_id)
        };
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
                    document_path: format!("datasets/bench-{label}-{writer}-{index}"),
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
    initialize_task_incoming(
        context.clone(),
        task_handle.clone(),
        aruna_operations::jobs::runtime::JobsRuntime::new(),
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
