//! Times fixed workloads through the runner on real backends in temporary folders and prints rates.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Lints overflow the default query depth on the nested async writer tasks, as in tests/main.rs.
#![recursion_limit = "512"]

#[path = "../tests/convergence/mod.rs"]
mod convergence;
#[path = "../tests/topology/mod.rs"]
mod topology;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aruna_blob::blob::BlobHandler;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{API_STATE_KEYSPACE, NOTIFICATION_OUTBOX_KEYSPACE};
use aruna_core::shutdown::Shutdown;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::execution::notification::{
    NotificationClass, NotificationKind, NotificationRecord,
};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::structs::storage::blob::{Backend, BackendConfig, BucketInfo};
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::routing::RoutingSnapshot;
use aruna_core::time::unix_timestamp_millis;
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
use aruna_operations::notifications::emit::{EmitNotificationsInput, EmitNotificationsOperation};
use aruna_operations::placement::process_placements::process_shard_placements;
use aruna_operations::realm::announce_presence::{
    AnnouncePresenceConfig, AnnouncePresenceOperation,
};
use aruna_operations::realm::get_nodes::GetNodesOperation;
use aruna_operations::s3::bucket::create::CreateBucketOperation;
use aruna_operations::s3::object::get::{GetObjectInput, GetObjectOperation};
use aruna_operations::s3::object::put::{PutObjectConfig, PutObjectInput, PutObjectOperation};
use aruna_operations::sync::incoming::initialize_incoming_fixture;
use aruna_operations::tasks::incoming::{drain_notification_outbox, start_task_queues};
use aruna_storage::{FjallStorage, StorageHandle};
use aruna_tasks::TaskHandle;
use convergence::{
    NO_PROGRESS_TIMEOUT, TestResult, wait_empty_queues, wait_for_convergence, wait_for_visibility,
};
use futures_util::StreamExt;
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
const OUTBOX_SIZE: usize = 10_000;
const EMIT_BATCH: usize = 100;
const STORAGE_ROWS: usize = 100_000;
const WRITE_BATCH: usize = 1000;
const READ_BATCH: usize = 100;
const OBJECT_COUNT: usize = 500;
const OBJECT_BYTES: usize = 4096;
const BUCKET: &str = "measure";

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
    measure_outbox().await?;
    let single = spawn_single().await?;
    measure_reads(&single.context).await?;
    measure_objects(&single).await?;
    single.net.shutdown().await;
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

/// Emits OUTBOX_SIZE notifications on one node with its timers stopped, then times
/// explicit drains until the outbox is empty. Recipients spread over all holders.
async fn measure_outbox() -> TestResult<()> {
    let realm = build_realm(126).await?;
    let result = drain_outbox(&realm).await;
    shutdown_realm(realm).await;
    let seconds = result?;
    report("notification_drain", "nodes=3", OUTBOX_SIZE, seconds);
    Ok(())
}

async fn drain_outbox(realm: &Realm) -> TestResult<f64> {
    let context = realm.nodes[0].context.clone();
    context
        .task_handle
        .as_ref()
        .ok_or("the node needs a task handle")?
        .clear_inbound_handler()
        .await;
    for _ in 0..OUTBOX_SIZE / EMIT_BATCH {
        let records = (0..EMIT_BATCH)
            .map(|_| notification(realm.realm_id))
            .collect();
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

async fn outbox_rows(storage: &StorageHandle) -> TestResult<usize> {
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
async fn measure_reads(context: &DriverContext) -> TestResult<()> {
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
async fn measure_objects(single: &Node) -> TestResult<()> {
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

/// One node outside any realm with production storage and a file system blob backend.
async fn spawn_single() -> TestResult<Node> {
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
        task_handle: None,
        compute_handle: None,
    });
    Ok(Node {
        _dir: dir,
        _shutdown: Shutdown::new(),
        net,
        context,
    })
}

async fn shutdown_realm(realm: Realm) {
    for node in realm.nodes {
        node.net.shutdown().await;
    }
}
