//! Builds storage-only driver contexts, a local net config, raw storage reads and writes, and
//! signed tokens for tests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::document::{DocumentOutboxRecord, DocumentReconcileResult};
use aruna_core::document_apply::ApplyError;
use aruna_core::document_apply::reconcile::ReconcileTopicsOperation;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::row_plan::PlanOperation;
use aruna_core::structs::identity::auth::TokenClaims;
use aruna_core::structs::identity::realm::RealmId;
use aruna_net::{DiscoveryMethod, NetConfig, RelayMethod};
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};

use crate::driver::{DriverContext, drive};
use crate::sync::document_outbox::{
    OutboxPagePlan, OutboxReadBatch, OutboxTailsOperation, OutboxTimerOperation, ReadOutboxPlan,
};
use crate::sync::reconcile::SyncTopicsOperation;

/// A driver context with storage and no other plane.
pub fn test_context(storage: StorageHandle) -> DriverContext {
    DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    }
}

/// A storage driver context with a task plane, shared behind an `Arc`.
pub fn task_context(storage: StorageHandle) -> std::sync::Arc<DriverContext> {
    std::sync::Arc::new(DriverContext {
        task_handle: Some(TaskHandle::new()),
        ..test_context(storage)
    })
}

/// A storage-only driver context on a store opened at `root`.
pub fn path_context(root: &str) -> DriverContext {
    test_context(aruna_storage::FjallStorage::open(root).expect("open the test store"))
}

/// A net on loopback without relays or discovery, so a test reaches no outside service. A relay
/// connection's timers also read the real clock and spin forever under paused time.
pub fn local_net() -> NetConfig {
    NetConfig {
        bind_addr: "127.0.0.1:0".parse().expect("loopback address"),
        discovery_method: DiscoveryMethod::None,
        relay_method: RelayMethod::None,
        ..NetConfig::default()
    }
}

/// Writes `writes` in one batch outside a transaction; panics on any other answer.
pub async fn write_entries(storage: &StorageHandle, writes: Vec<(String, ByteView, ByteView)>) {
    match storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
        other => panic!("unexpected storage event: {other:?}"),
    }
}

/// The stored value of `key`; panics on any answer but a read result.
pub async fn read_value(storage: &StorageHandle, keyspace: &str, key: Vec<u8>) -> Option<ByteView> {
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = storage
        .send_storage_effect(StorageEffect::Read {
            key_space: keyspace.to_string(),
            key: key.into(),
            txn_id: None,
        })
        .await
    else {
        panic!("unexpected storage read event")
    };
    value
}

/// The outbox record stored at `key`.
pub async fn read_outbox_record(
    storage: &StorageHandle,
    key: &[u8],
) -> Result<Option<DocumentOutboxRecord>, ApplyError> {
    let plan = ReadOutboxPlan { key: key.to_vec() };
    drive(PlanOperation::new(plan), &test_context(storage.clone())).await
}

/// One page of outbox records under `prefix`.
pub async fn read_outbox_records(
    storage: &StorageHandle,
    prefix: &[u8],
    start_after: Option<Vec<u8>>,
    limit: usize,
) -> Result<OutboxReadBatch, ApplyError> {
    let plan = OutboxPagePlan {
        prefix: prefix.to_vec(),
        start_after,
        limit,
    };
    drive(PlanOperation::new(plan), &test_context(storage.clone())).await
}

/// The last key of every outbox stream that holds a record.
pub async fn read_outbox_tails(
    storage: &StorageHandle,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ApplyError> {
    drive(OutboxTailsOperation::new(), &test_context(storage.clone())).await
}

/// Arms the outbox drain on `tasks` as a restarting node does.
pub async fn restore_outbox_timers(storage: &StorageHandle, tasks: &TaskHandle) {
    let mut context = test_context(storage.clone());
    context.task_handle = Some(tasks.clone());
    drive(OutboxTimerOperation::new(), &context)
        .await
        .expect("outbox timer restores");
}

fn context_realm(context: &DriverContext) -> RealmId {
    *context
        .net_handle
        .as_ref()
        .expect("the context has a net handle")
        .realm_id()
}

/// Pulls `topics` from `peers` and reconciles them.
pub async fn sync_topics(
    context: &DriverContext,
    topics: Vec<irokle::TopicId>,
    peers: Vec<NodeId>,
) -> Result<DocumentReconcileResult, ApplyError> {
    let sync = SyncTopicsOperation::new(context_realm(context), topics, peers);
    drive(sync, context).await
}

/// Pulls one topic from `peers` and reconciles it; a topic that cannot be fetched fails.
pub async fn sync_topic(
    context: &DriverContext,
    topic: irokle::TopicId,
    peers: Vec<NodeId>,
) -> Result<DocumentReconcileResult, ApplyError> {
    let sync = SyncTopicsOperation::single(context_realm(context), topic, peers);
    drive(sync, context).await
}

/// Reconciles what `topics` already hold.
pub async fn reconcile_topics(
    context: &DriverContext,
    topics: Vec<irokle::TopicId>,
) -> Result<DocumentReconcileResult, ApplyError> {
    let reconcile = ReconcileTopicsOperation::new(context_realm(context), topics);
    drive(reconcile, context).await
}

/// An EdDSA bearer token of `claims` signed with `signing_key`.
pub fn sign_token(signing_key: &SigningKey, claims: &TokenClaims) -> String {
    let key_pem = signing_key.to_pkcs8_pem(LineEnding::LF).unwrap();
    encode(
        &Header::new(Algorithm::EdDSA),
        claims,
        &EncodingKey::from_ed_pem(key_pem.as_bytes()).unwrap(),
    )
    .unwrap()
}

/// Replays the whole event log into the registry projection.
pub async fn replay_event_log(
    context: &DriverContext,
) -> Result<usize, crate::metadata::projection::MetadataProjectionError> {
    use crate::metadata::projection::{ReplayPageOperation, max_clock_skew};
    let local_node = context.net_handle.as_ref().map(|net| net.node_id());
    let mut after = None;
    let mut projected = 0usize;
    loop {
        let page = ReplayPageOperation::new(after, local_node, max_clock_skew());
        let (count, next) = crate::driver::drive(page, context).await?;
        projected = projected.saturating_add(count);
        match next {
            Some(next) => after = Some(next),
            None => return Ok(projected),
        }
    }
}

/// Runs one pass of the pending projection drain.
pub async fn drain_projection_queue(
    context: &DriverContext,
) -> Result<
    crate::metadata::projection::PendingProjectionResult,
    crate::metadata::projection::MetadataProjectionError,
> {
    use crate::metadata::projection::{DrainProjectionOperation, max_clock_skew};
    let local_node = context.net_handle.as_ref().map(|net| net.node_id());
    let drain = DrainProjectionOperation::new(local_node, max_clock_skew());
    crate::driver::drive(drain, context).await
}

/// Projects carried events.
pub async fn project_create_events(
    context: &DriverContext,
    events: Vec<aruna_core::metadata::MetadataEventRecord>,
    local_node_id: Option<aruna_core::NodeId>,
) -> Result<usize, crate::metadata::projection::MetadataProjectionError> {
    use crate::metadata::projection::{ProjectEventsOperation, max_clock_skew};
    let project = ProjectEventsOperation::new(events, local_node_id, max_clock_skew());
    crate::driver::drive(project, context).await
}

/// Projects logged events by their document and event ids.
pub async fn project_logged_events(
    context: &DriverContext,
    targets: impl IntoIterator<Item = (ulid::Ulid, ulid::Ulid)>,
) -> Result<usize, crate::metadata::projection::MetadataProjectionError> {
    use crate::metadata::projection::{ProjectLoggedOperation, max_clock_skew};
    let local_node = context.net_handle.as_ref().map(|net| net.node_id());
    let project = ProjectLoggedOperation::new(targets, false, local_node, max_clock_skew());
    Ok(crate::driver::drive(project, context).await?.projected)
}

/// Projects one logged event.
pub async fn project_logged_event(
    context: &DriverContext,
    document_id: ulid::Ulid,
    event_id: ulid::Ulid,
) -> Result<(), crate::metadata::projection::MetadataProjectionError> {
    project_logged_events(context, [(document_id, event_id)])
        .await
        .map(|_| ())
}

/// Wakes the projection drain after `after`.
pub async fn schedule_projection_drain(
    context: &DriverContext,
    after: std::time::Duration,
) -> Result<(), crate::metadata::projection::MetadataProjectionError> {
    let wake = crate::metadata::projection::ProjectionWakeOperation::new(after);
    crate::driver::drive(wake, context).await
}

/// Runs one pass of the metadata materialization drain, as the drain task does.
pub async fn process_materialization_batch(
    context: &DriverContext,
) -> Result<
    crate::metadata::materialization_queue::MetadataDrainResult,
    crate::metadata::materialization_queue::MetadataMaterializationError,
> {
    use crate::metadata::materialization_queue::{
        MaterializationBatchOperation, materialization_group_concurrency,
    };
    let node = crate::git::GitNode {
        ids: context.node_and_realm(),
        routing: crate::driver::node_routing(context),
    };
    let concurrency = materialization_group_concurrency();
    let batch =
        MaterializationBatchOperation::new(node, concurrency, context.task_handle.is_some());
    drive(batch, context).await
}

/// Whether a live metadata materialization job is queued; dead ones it meets are deleted.
pub async fn materialization_jobs_exist(
    context: &DriverContext,
) -> Result<bool, crate::metadata::materialization_queue::MetadataMaterializationError> {
    let probe = crate::metadata::materialization_queue::JobsExistOperation::new();
    drive(probe, context).await
}

/// Runs one pass of the graph prune drain.
pub async fn process_prune_batch(
    context: &DriverContext,
) -> Result<
    crate::metadata::prune_queue::GraphDrainResult,
    crate::metadata::prune_queue::MetadataGraphError,
> {
    let batch = crate::metadata::prune_queue::PruneBatchOperation::new(context.node_scope());
    crate::driver::drive(batch, context).await
}

/// Queues and runs the prunes for deleted graphs.
pub async fn process_graph_tombstones(
    context: &DriverContext,
    tombstones: impl IntoIterator<Item = aruna_core::metadata::GraphLifecycleRecord>,
) -> crate::metadata::prune_queue::TombstoneProcessingResult {
    let tombstones = crate::metadata::prune_queue::TombstonesOperation::new(tombstones);
    crate::driver::drive(tombstones, context)
        .await
        .unwrap_or_default()
}

/// Whether any graph prune job is queued.
pub async fn prune_jobs_exist(
    context: &DriverContext,
) -> Result<bool, crate::metadata::prune_queue::MetadataGraphError> {
    let due = crate::driver::drive(crate::metadata::prune_queue::jobs_queued(), context).await?;
    Ok(!due.jobs.is_empty())
}

pub struct RecordingTaskHandler {
    pub seen: tokio::sync::mpsc::Sender<aruna_core::task::TaskKey>,
}

#[async_trait::async_trait]
impl aruna_tasks::InboundTaskHandler for RecordingTaskHandler {
    async fn handle_timer(&self, key: aruna_core::task::TaskKey) {
        let _ = self.seen.send(key).await;
    }
}

/// Tokio inhibits paused-clock auto-advance while a blocking task is alive.
/// Storage and net answer from their own threads, so without this the clock
/// races ahead of every round trip. `tokio::time::advance` still applies.
pub struct ClockGuard {
    _stop: std::sync::mpsc::Sender<()>,
}

pub fn freeze_clock() -> ClockGuard {
    let (stop, wait) = std::sync::mpsc::channel::<()>();
    tokio::task::spawn_blocking(move || {
        let _ = wait.recv();
    });
    ClockGuard { _stop: stop }
}

/// The graph handle a test installed on its context.
pub fn installed_graph_handle(context: &DriverContext) -> &aruna_graph::GraphHandle {
    context
        .graph_handle
        .as_ref()
        .expect("graph handle installed")
}
