//! Times fixed workloads through the runner on real backends in temporary folders and prints rates.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Lints overflow the default query depth on the nested async writer tasks, as in tests/main.rs.
#![recursion_limit = "512"]

#[path = "../tests/convergence/mod.rs"]
mod convergence;
#[path = "../tests/topology/mod.rs"]
mod topology;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use aruna_blob::blob::BlobHandler;
use aruna_core::effects::{Effect, EffectGroup, GroupForm, StorageEffect};
use aruna_core::events::{Event, GroupEvent, GroupResult, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{API_STATE_KEYSPACE, NOTIFICATION_OUTBOX_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::execution::notification::{
    NotificationClass, NotificationKind, NotificationRecord,
};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{Backend, BackendConfig, BucketInfo};
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::routing::RoutingSnapshot;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{Effects, GroupId};
use aruna_core::{NodeId, StructuredId, UserId};
use aruna_net::{NetConfig, NetHandle};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::metadata::create_document::{
    CreateDocumentConfig, CreateDocumentOperation, mint_local_document,
};
use aruna_operations::metadata::get_document::GetDocumentOperation;
use aruna_operations::metadata::projector::project_logged_events;
use aruna_operations::notifications::emit::{EmitNotificationsInput, EmitNotificationsOperation};
use aruna_operations::realm::get_config::GetConfigOperation;
use aruna_operations::s3::bucket::create::CreateBucketOperation;
use aruna_operations::s3::object::get::{GetObjectInput, GetObjectOperation};
use aruna_operations::s3::object::put::{PutObjectConfig, PutObjectInput, PutObjectOperation};
use aruna_operations::tasks::incoming::drain_notification_outbox;
use aruna_storage::{FjallStorage, StorageHandle};
use convergence::{NO_PROGRESS_TIMEOUT, wait_empty_queues, wait_for_visibility};
use futures_util::StreamExt;
use tempfile::TempDir;
use tokio::time::{MissedTickBehavior, sleep};
use topology::{
    BoxError, TestNode, WriterSettings, build_realm_nodes, init_logging, make_runtime,
    mixed_payload, node_targets, run_writers, scaffold_payload, shutdown_nodes,
};
use ulid::Ulid;

const REALM_NODES: usize = 3;
const TOTAL_CREATES: usize = 2000;
const WRITER_LEVELS: [usize; 4] = [1, 8, 32, 64];
const SPREAD_WRITERS: usize = 64;
const SPREAD_DOCS: usize = 16;
const SAMPLE_DOCS: usize = 100;
const DRAIN_WRITERS: usize = 32;
const DRAIN_DOCS: usize = 128;
const TAIL_WRITERS: usize = 24;
const TAIL_DOCS: usize = 500;
const TAIL_PERIOD: Duration = Duration::from_millis(40);
const PROBE_PERIOD: Duration = Duration::from_millis(500);
const PROBE_POLL: Duration = Duration::from_millis(50);
const PROBE_TIMEOUT: Duration = Duration::from_secs(60);
const VISIBILITY_POLL: Duration = Duration::from_millis(200);
const OUTBOX_SIZE: usize = 10_000;
const EMIT_BATCH: usize = 100;
const STORAGE_ROWS: usize = 100_000;
const WRITE_BATCH: usize = 1000;
const READ_BATCH: usize = 100;
const OBJECT_COUNT: usize = 500;
const OBJECT_BYTES: usize = 4096;
const BUCKET: &str = "measure";
const RUNNER_RUNS: usize = 20_000;
const RUNNER_READS: usize = 16;

/// The single node of the storage and object workloads, kept alive with its folder.
struct Node {
    _dir: TempDir,
    net: NetHandle,
    context: Arc<DriverContext>,
}

/// Workloads in run order; each runs in its own child process, so the nodes, stores and
/// background tasks of one workload cannot slow down or starve the next one.
const WORKLOADS: [&str; 10] = [
    "create-1",
    "create-8",
    "create-32",
    "create-64",
    "spread",
    "converge",
    "tail",
    "outbox",
    "single",
    "runner",
];

fn main() -> Result<(), BoxError> {
    init_logging();
    match std::env::args().nth(1) {
        Some(workload) => run_child(&workload),
        None => run_each(),
    }
}

fn run_each() -> Result<(), BoxError> {
    let program = std::env::current_exe()?;
    for workload in WORKLOADS {
        let status = std::process::Command::new(&program)
            .arg(workload)
            .status()?;
        if !status.success() {
            return Err(format!("workload {workload} failed: {status}").into());
        }
    }
    Ok(())
}

fn run_child(workload: &str) -> Result<(), BoxError> {
    let runtime = make_runtime()?;
    let result = runtime.block_on(run_workload(workload));
    runtime.shutdown_timeout(Duration::from_secs(10));
    result
}

async fn run_workload(workload: &str) -> Result<(), BoxError> {
    if let Some(writers) = workload.strip_prefix("create-") {
        let writers: usize = writers.parse()?;
        let level = WRITER_LEVELS
            .iter()
            .position(|&level| level == writers)
            .ok_or("unknown writer level")?;
        return measure_creates(91 + level as u8, writers).await;
    }
    match workload {
        "spread" => measure_spread().await,
        "converge" => measure_drain().await,
        "tail" => measure_tail().await,
        "outbox" => measure_outbox().await,
        "runner" => measure_runner().await,
        "single" => {
            let single = spawn_single().await?;
            let result = match measure_reads(&single.context).await {
                Ok(()) => measure_objects(&single).await,
                Err(error) => Err(error),
            };
            single.net.shutdown().await;
            result
        }
        other => Err(format!("unknown workload {other}").into()),
    }
}

fn report(workload: &str, detail: &str, count: usize, seconds: f64) {
    let rate = count as f64 / seconds;
    println!("{workload} {detail} count={count} seconds={seconds:.3} per_second={rate:.1}");
}

fn contexts(nodes: &[TestNode]) -> Vec<Arc<DriverContext>> {
    nodes.iter().map(|node| node.context.clone()).collect()
}

fn pairs_of(created: &[(GroupId, Ulid, Instant)]) -> Vec<(GroupId, Ulid)> {
    created
        .iter()
        .map(|(group_id, document_id, _)| (*group_id, *document_id))
        .collect()
}

/// Creates and projects TOTAL_CREATES documents with `writers` parallel writers.
async fn measure_creates(seed: u8, writers: usize) -> Result<(), BoxError> {
    let realm_id = RealmId([seed; 32]);
    let nodes = build_realm_nodes(&realm_id, REALM_NODES).await?;
    let targets = node_targets(&nodes);
    let per_writer = TOTAL_CREATES / writers;
    let started = Instant::now();
    let created = run_writers(
        &WriterSettings {
            realm_id,
            group_id: Ulid::generate(),
            label: "measure".to_string(),
            period: None,
            payload: mixed_payload,
        },
        writers,
        per_writer,
        &targets,
    )
    .await;
    let seconds = started.elapsed().as_secs_f64();
    shutdown_nodes(nodes).await;
    report(
        "metadata_create",
        &format!("writers={writers}"),
        created?.len(),
        seconds,
    );
    Ok(())
}

/// Time from the last create until the last SAMPLE_DOCS documents read back on every node.
async fn measure_spread() -> Result<(), BoxError> {
    let realm_id = RealmId([122u8; 32]);
    let nodes = build_realm_nodes(&realm_id, REALM_NODES).await?;
    let targets = node_targets(&nodes);
    let created = run_writers(
        &WriterSettings {
            realm_id,
            group_id: Ulid::generate(),
            label: "spread".to_string(),
            period: None,
            payload: mixed_payload,
        },
        SPREAD_WRITERS,
        SPREAD_DOCS,
        &targets,
    )
    .await;
    let mut created = match created {
        Ok(created) => created,
        Err(error) => {
            shutdown_nodes(nodes).await;
            return Err(error);
        }
    };
    created.sort_by_key(|(_, _, at)| *at);
    let newest = created.len().saturating_sub(SAMPLE_DOCS);
    let sample = pairs_of(&created[newest..]);
    let started = Instant::now();
    let result = wait_for_visibility(&contexts(&nodes), &sample, VISIBILITY_POLL).await;
    let seconds = started.elapsed().as_secs_f64();
    shutdown_nodes(nodes).await;
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
async fn measure_drain() -> Result<(), BoxError> {
    let realm_id = RealmId([124u8; 32]);
    let nodes = build_realm_nodes(&realm_id, REALM_NODES).await?;
    let result = converge_all(realm_id, &nodes).await;
    shutdown_nodes(nodes).await;
    let (docs, seconds) = result?;
    report(
        "metadata_converge",
        &format!("writers={DRAIN_WRITERS}"),
        docs,
        seconds,
    );
    Ok(())
}

async fn converge_all(realm_id: RealmId, nodes: &[TestNode]) -> Result<(usize, f64), BoxError> {
    let targets = node_targets(nodes);
    let started = Instant::now();
    let created = run_writers(
        &WriterSettings {
            realm_id,
            group_id: Ulid::generate(),
            label: "drain".to_string(),
            period: None,
            payload: mixed_payload,
        },
        DRAIN_WRITERS,
        DRAIN_DOCS,
        &targets,
    )
    .await?;
    let pairs = pairs_of(&created);
    let all = contexts(nodes);
    wait_for_visibility(&all, &pairs, VISIBILITY_POLL).await?;
    wait_empty_queues(&all).await?;
    Ok((pairs.len(), started.elapsed().as_secs_f64()))
}

/// The propagation tail load: paced writers while a sampler creates one probe document
/// every PROBE_PERIOD and times until it reads back on every node.
async fn measure_tail() -> Result<(), BoxError> {
    let realm_id = RealmId([125u8; 32]);
    let nodes = build_realm_nodes(&realm_id, REALM_NODES).await?;
    let result = sample_tail(realm_id, &nodes).await;
    shutdown_nodes(nodes).await;
    result
}

async fn sample_tail(realm_id: RealmId, nodes: &[TestNode]) -> Result<(), BoxError> {
    let targets = node_targets(nodes);
    let all = contexts(nodes);
    let group_id = Ulid::generate();
    let (done_tx, done_rx) = tokio::sync::watch::channel(false);
    let sampler = tokio::spawn(run_sampler(
        realm_id,
        group_id,
        targets.clone(),
        all.clone(),
        done_rx,
    ));
    let started = Instant::now();
    let created = run_writers(
        &WriterSettings {
            realm_id,
            group_id,
            label: "tail".to_string(),
            period: Some(TAIL_PERIOD),
            payload: mixed_payload,
        },
        TAIL_WRITERS,
        TAIL_DOCS,
        &targets,
    )
    .await;
    let ingest_seconds = started.elapsed().as_secs_f64();
    let _ = done_tx.send(true);
    let pairs = pairs_of(&created?);
    let convergence = async {
        wait_for_visibility(&all, &pairs, VISIBILITY_POLL).await?;
        wait_empty_queues(&all).await?;
        Ok::<f64, String>(started.elapsed().as_secs_f64())
    };
    let (sampled, converged) = tokio::join!(sampler, convergence);
    let (mut latencies, failures) = sampled??;
    let converge_seconds = converged?;
    latencies.sort_unstable();
    println!(
        "metadata_tail probes={} failures={failures} p50_ms={} p90_ms={} p95_ms={} max_ms={}",
        latencies.len() + failures,
        percentile(&latencies, 50.0),
        percentile(&latencies, 90.0),
        percentile(&latencies, 95.0),
        latencies.last().copied().unwrap_or(0),
    );
    let detail = format!("writers={TAIL_WRITERS} ingest_seconds={ingest_seconds:.3}");
    report("metadata_sustained", &detail, pairs.len(), converge_seconds);
    Ok(())
}

fn percentile(sorted: &[u128], pct: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = ((sorted.len() as f64) * pct / 100.0).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

/// Creates one probe document per PROBE_PERIOD until ingest ends and returns the
/// milliseconds until each read back on every node, plus the probes that never did.
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
    let config = drive(GetConfigOperation::new(realm_id), targets[0].1.as_ref())
        .await
        .map_err(|error| format!("realm config load failed: {error:?}"))?;
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
        let document_path = format!("probe/tail-{index}");
        let actor = Actor {
            node_id,
            user_id: UserId::local(Ulid::generate(), realm_id),
            realm_id,
        };
        let document_id = mint_local_document(&config, &actor, group_id, &document_path)
            .map_err(|error| format!("mint failed index={index}: {error:?}"))?
            .as_ulid();
        let t0 = Instant::now();
        let result = drive(
            CreateDocumentOperation::new_generated_id(CreateDocumentConfig {
                actor,
                group_id,
                document_id,
                document_path,
                public: true,
                payload: scaffold_payload("probe", 0, index),
            }),
            context.as_ref(),
        )
        .await
        .map_err(|error| format!("probe create failed index={index}: {error:?}"))?;
        project_logged_events(
            context.as_ref(),
            vec![(result.record.document_id, result.record.last_event_id)],
        )
        .await
        .map_err(|error| format!("probe projection failed index={index}: {error:?}"))?;

        let probe_id = result.record.document_id;
        let contexts = contexts.clone();
        waiters.push(tokio::spawn(async move {
            wait_all_visible(&contexts, group_id, probe_id, t0).await
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

async fn wait_all_visible(
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
                GetDocumentOperation::new(group_id, document_id),
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

/// Emits OUTBOX_SIZE notifications on one node with its timers stopped, then times
/// explicit drains until the outbox is empty. Recipients spread over all holders.
async fn measure_outbox() -> Result<(), BoxError> {
    let realm_id = RealmId([126u8; 32]);
    let nodes = build_realm_nodes(&realm_id, REALM_NODES).await?;
    let result = drain_outbox(realm_id, &nodes).await;
    shutdown_nodes(nodes).await;
    let seconds = result?;
    report("notification_drain", "nodes=3", OUTBOX_SIZE, seconds);
    Ok(())
}

async fn drain_outbox(realm_id: RealmId, nodes: &[TestNode]) -> Result<f64, BoxError> {
    let context = nodes[0].context.clone();
    context
        .task_handle
        .as_ref()
        .ok_or("the node needs a task handle")?
        .clear_inbound_handler()
        .await;
    for _ in 0..OUTBOX_SIZE / EMIT_BATCH {
        let records = (0..EMIT_BATCH).map(|_| notification(realm_id)).collect();
        drive(
            EmitNotificationsOperation::new(EmitNotificationsInput { records }),
            context.as_ref(),
        )
        .await
        .map_err(|error| format!("emit failed: {error:?}"))?;
    }
    let queued = outbox_rows(&context.storage_handle).await?;
    if queued != OUTBOX_SIZE {
        return Err(format!("outbox holds {queued} rows, expected {OUTBOX_SIZE}").into());
    }
    let started = Instant::now();
    let mut best = queued;
    let mut deadline = Instant::now() + NO_PROGRESS_TIMEOUT;
    loop {
        drain_notification_outbox(context.clone()).await;
        let left = outbox_rows(&context.storage_handle).await?;
        if left == 0 {
            return Ok(started.elapsed().as_secs_f64());
        }
        if left < best {
            best = left;
            deadline = Instant::now() + NO_PROGRESS_TIMEOUT;
        }
        if Instant::now() >= deadline {
            return Err(format!("outbox drain stalled with {left} rows").into());
        }
    }
}

fn notification(realm_id: RealmId) -> NotificationRecord {
    NotificationRecord::new(
        UserId::local(Ulid::generate(), realm_id),
        NotificationClass::Direct,
        NotificationKind::AddedToGroup {
            group_id: Ulid::generate(),
            actor_user_id: UserId::local(Ulid::generate(), realm_id),
        },
        unix_timestamp_millis(),
    )
}

async fn outbox_rows(storage: &StorageHandle) -> Result<usize, BoxError> {
    let scan = StorageEffect::Iter {
        key_space: NOTIFICATION_OUTBOX_KEYSPACE.to_string(),
        prefix: None,
        start: None,
        limit: OUTBOX_SIZE + 1,
        txn_id: None,
    };
    match storage.send_effect(Effect::Storage(scan)).await {
        Event::Storage(StorageEvent::IterResult { values, .. }) => Ok(values.len()),
        other => Err(format!("unexpected outbox scan event: {other:?}").into()),
    }
}

/// Writes STORAGE_ROWS small rows, then times reading them back in batches of READ_BATCH.
async fn measure_reads(context: &DriverContext) -> Result<(), BoxError> {
    let keys: Vec<Vec<u8>> = (0..STORAGE_ROWS)
        .map(|index| format!("measure/{index:08}").into_bytes())
        .collect();
    for chunk in keys.chunks(WRITE_BATCH) {
        let writes = chunk
            .iter()
            .map(|key| {
                (
                    API_STATE_KEYSPACE.to_string(),
                    key.clone().into(),
                    vec![7u8; 64].into(),
                )
            })
            .collect();
        let effect = StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        };
        match context
            .storage_handle
            .send_effect(Effect::Storage(effect))
            .await
        {
            Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
            other => return Err(format!("unexpected batch write event: {other:?}").into()),
        }
    }
    let started = Instant::now();
    for chunk in keys.chunks(READ_BATCH) {
        let reads = chunk
            .iter()
            .map(|key| (API_STATE_KEYSPACE.to_string(), key.clone().into()))
            .collect();
        let effect = StorageEffect::BatchRead {
            reads,
            txn_id: None,
        };
        match context
            .storage_handle
            .send_effect(Effect::Storage(effect))
            .await
        {
            Event::Storage(StorageEvent::BatchReadResult { values })
                if values.iter().all(|(_, value)| value.is_some()) => {}
            other => return Err(format!("unexpected batch read event: {other:?}").into()),
        }
    }
    let seconds = started.elapsed().as_secs_f64();
    report(
        "storage_batch_read",
        &format!("batch={READ_BATCH}"),
        STORAGE_ROWS,
        seconds,
    );
    Ok(())
}

/// Times OBJECT_COUNT puts and then OBJECT_COUNT full reads of small objects.
async fn measure_objects(single: &Node) -> Result<(), BoxError> {
    let realm_id = RealmId::from_bytes([7u8; 32]);
    let user_id = UserId::local(Ulid::generate(), realm_id);
    let node_id = single.net.node_id();
    let group_id = Ulid::generate();
    let info = BucketInfo {
        group_id,
        created_at: std::time::SystemTime::now(),
        created_by: user_id,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: None,
    };
    drive(
        CreateBucketOperation::new(BUCKET.to_string(), info),
        &single.context,
    )
    .await
    .map_err(|error| format!("bucket create failed: {error:?}"))?;
    let data = vec![42u8; OBJECT_BYTES];
    let started = Instant::now();
    for index in 0..OBJECT_COUNT {
        let body: BackendStream<Result<bytes::Bytes, StreamError>> = BackendStream::new(
            tokio_util::io::ReaderStream::new(std::io::Cursor::new(data.clone())),
        );
        let put = PutObjectConfig {
            user_id,
            group_id,
            realm_id,
            node_id,
            request: PutObjectInput {
                bucket: BUCKET.to_string(),
                key: format!("objects/{index:06}"),
                content_length: Some(OBJECT_BYTES as u64),
                body: Some(body),
            },
            expected_checksums: Vec::new(),
            checksum_type: None,
            exists: false,
            version_source: None,
            preassigned_version_id: None,
            quota_ceiling: None,
            routing: RoutingSnapshot::single(group_id),
        };
        drive(PutObjectOperation::new(put), &single.context)
            .await
            .map_err(|error| format!("put failed: {error:?}"))?;
    }
    report(
        "s3_put",
        &format!("bytes={OBJECT_BYTES}"),
        OBJECT_COUNT,
        started.elapsed().as_secs_f64(),
    );
    let started = Instant::now();
    for index in 0..OBJECT_COUNT {
        let get = GetObjectInput {
            bucket: BUCKET.to_string(),
            key: format!("objects/{index:06}"),
            version_id: None,
            range: None,
            group_id,
            user_identity: user_id,
            realm_id,
            node_id,
        };
        let result = drive(GetObjectOperation::new(get), &single.context)
            .await
            .map_err(|error| format!("get failed: {error:?}"))?;
        let mut read = 0usize;
        let mut blob = result.blob.0;
        while let Some(chunk) = blob.next().await {
            read += chunk
                .map_err(|error| format!("read failed: {error:?}"))?
                .len();
        }
        if read != OBJECT_BYTES {
            return Err(format!("object {index} read {read} bytes").into());
        }
    }
    report(
        "s3_get",
        &format!("bytes={OBJECT_BYTES}"),
        OBJECT_COUNT,
        started.elapsed().as_secs_f64(),
    );
    Ok(())
}

/// One node outside any realm with production storage and a file system blob backend.
async fn spawn_single() -> Result<Node, BoxError> {
    let dir = tempfile::tempdir()?;
    let storage = FjallStorage::open(dir.path().to_str().ok_or("invalid storage path")?)?;
    let blob_root = dir.path().join("blobstore");
    std::fs::create_dir_all(&blob_root)?;
    let net = NetHandle::new(NetConfig::default(), storage.clone()).await?;
    let backend = BackendConfig {
        backend_type: Backend::FileSystem,
        root: blob_root.to_string_lossy().to_string(),
        service_config: HashMap::new(),
        bucket_prefix: Some("aruna_".to_string()),
        max_bucket_size: Some(100_000),
        multipart_bucket: Some("uploaded-parts".to_string()),
        timeouts: Default::default(),
    };
    let blob_handle = BlobHandler::new(backend, storage.clone(), net.clone()).await?;
    let context = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: Some(net.clone()),
        blob_handle: Some(blob_handle),
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    Ok(Node {
        _dir: dir,
        net,
        context,
    })
}

/// Reads `reads` stored rows, one after another or as one effect group.
#[derive(Debug, PartialEq)]
struct ReadRows {
    reads: usize,
    grouped: bool,
    answered: usize,
    failed: bool,
}

fn runner_read(index: usize) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: API_STATE_KEYSPACE.to_string(),
        key: format!("runner/{index:04}").into_bytes().into(),
        txn_id: None,
    })
}

impl Operation for ReadRows {
    type Output = ();
    type Error = ();

    fn start(&mut self) -> Effects {
        let effect = match self.grouped {
            true => Effect::Group(EffectGroup {
                members: (0..self.reads).map(runner_read).collect(),
                limit: self.reads,
                form: GroupForm::All { deadline: None },
            }),
            false => runner_read(0),
        };
        smallvec::smallvec![effect]
    }

    fn step(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::ReadResult { value: Some(_), .. }) if !self.grouped => {
                self.answered += 1;
            }
            Event::Group(GroupEvent::Finished { results }) if self.grouped => {
                self.answered = results
                    .iter()
                    .filter(|result| matches!(result, GroupResult::Done(_)))
                    .count();
                self.failed = self.answered < self.reads;
            }
            _ => self.failed = true,
        }
        match self.is_complete() {
            true => Effects::new(),
            false => smallvec::smallvec![runner_read(self.answered)],
        }
    }

    fn is_complete(&self) -> bool {
        self.failed || self.answered >= self.reads
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.failed {
            true => Err(()),
            false => Ok(()),
        }
    }

    fn abort(&mut self) -> Effects {
        self.failed = true;
        Effects::new()
    }
}

/// Times the runner on a storage-only node: a one-read operation, RUNNER_READS reads one after
/// another, and the same reads as one effect group.
async fn measure_runner() -> Result<(), BoxError> {
    let dir = tempfile::tempdir()?;
    let storage = FjallStorage::open(dir.path().to_str().ok_or("invalid storage path")?)?;
    let writes = (0..RUNNER_READS)
        .map(|index| {
            let key = format!("runner/{index:04}").into_bytes();
            (
                API_STATE_KEYSPACE.to_string(),
                key.into(),
                vec![7u8; 64].into(),
            )
        })
        .collect();
    let effect = StorageEffect::BatchWrite {
        writes,
        txn_id: None,
    };
    match storage.send_effect(Effect::Storage(effect)).await {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
        other => return Err(format!("unexpected batch write event: {other:?}").into()),
    }
    let context = DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
    };
    for (detail, reads, grouped) in [
        ("short", 1, false),
        ("sequence", RUNNER_READS, false),
        ("group", RUNNER_READS, true),
    ] {
        let started = Instant::now();
        for _ in 0..RUNNER_RUNS {
            let operation = ReadRows {
                reads,
                grouped,
                answered: 0,
                failed: false,
            };
            drive(operation, &context)
                .await
                .map_err(|()| format!("runner {detail} failed"))?;
        }
        let seconds = started.elapsed().as_secs_f64();
        report(
            "runner",
            &format!("{detail} reads={reads}"),
            RUNNER_RUNS,
            seconds,
        );
    }
    Ok(())
}
