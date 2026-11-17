//! Handles inbound sync traffic and applies reconciled documents and replication requests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use crate::driver::DriverContext;
use crate::jobs::runtime::JobsRuntime;
use crate::metadata::MetadataHandle;
use crate::metadata::projector::{
    PROJECTION_RETRY_AFTER, project_create_events, project_logged_events, schedule_projection_drain,
};
use crate::metadata::prune_queue::process_graph_tombstones;
use crate::node::dashboard::{notify_dashboard_change, targets_change_dashboard};
use crate::node::usage_stats::refresh_usage_targets;
use crate::notifications::watch::interest::refresh_target_interest;
use crate::placement::process_placements::reconcile_shard_topics;
use crate::replication::bootstrap::wake_bootstrap;
use crate::sync::outbox_record::{
    new_identified_record, schedule_drain_effect, write_outbox_effect,
};
use crate::tasks::queue_backoff::retry_delay_ms;
use aruna_core::alpn::Alpn;
use aruna_core::document::{DocumentEvictedDocument, DocumentReconcileResult, DocumentTarget};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::id::NodeId;
use aruna_core::shutdown::Shutdown;
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::task::{TaskEvent, TaskKey};
use aruna_core::telemetry::{QUEUE_LAG_INTERVAL, duration_ms};
use aruna_net::InboundEventHandler;
use aruna_net::streams::BiStream;
use async_trait::async_trait;
use tokio::time::sleep;
use tracing::{Instrument, debug, error, info, info_span, trace, warn};

const SYNC_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(60);
const MAINTENANCE_JITTER_SECS: u64 = 15;

#[derive(Debug)]
struct OperationsInboundHandler {
    context: Arc<DriverContext>,
    document_sync_reconcile: Arc<DocumentReconcileCoalescer>,
    rocrate_limits: RoCrateLimits,
    jobs_runtime: Arc<JobsRuntime>,
}

impl OperationsInboundHandler {
    fn new(
        context: Arc<DriverContext>,
        rocrate_limits: RoCrateLimits,
        jobs_runtime: Arc<JobsRuntime>,
        shutdown: Shutdown,
    ) -> Self {
        let document_sync_reconcile = Arc::new(DocumentReconcileCoalescer::new(shutdown.clone()));
        spawn_queue_gauge(Arc::downgrade(&document_sync_reconcile), &shutdown);
        Self {
            context,
            document_sync_reconcile,
            rocrate_limits,
            jobs_runtime,
        }
    }

    /// Reconciles the topics an admitted document sync stream touched; refusals
    /// fall back to the coalesced reconcile without panicking the peer.
    async fn accept_document_sync(&self, stream: BiStream, node_id: NodeId) {
        let Some(net_handle) = self.context.net_handle.clone() else {
            warn!(peer = %node_id, "Dropping inbound document sync stream without net handle");
            return;
        };
        match net_handle.handle_sync_stream(stream, node_id).await {
            Ok(touched_topics) => {
                self.document_sync_reconcile
                    .trigger(self.context.clone(), touched_topics);
            }
            Err(err) if err.is_admission_rejection() => {
                // Refused before any document was applied, so no partial
                // local state exists; dropping avoids an all-topic reconcile.
                debug!(peer = %node_id, error = ?err, "Dropped inbound document sync stream at admission");
            }
            Err(err) => {
                error!(error = ?err, "Failed to process inbound document sync stream");
                self.document_sync_reconcile
                    .trigger_all(self.context.clone());
            }
        }
    }

    /// Forwards an inbound metadata stream under the rocrate byte limit held by
    /// this handler; the metadata handle answers the peer itself.
    async fn handle_metadata_stream(&self, stream: BiStream, node_id: NodeId) {
        let Some(metadata_handle) = self.context.metadata_handle.clone() else {
            warn!(peer = %node_id, "Dropping inbound metadata stream without metadata handle");
            return;
        };
        if let Err(err) = metadata_handle
            .handle_inbound_stream(
                &self.context,
                stream,
                node_id,
                self.rocrate_limits.metadata_bytes,
            )
            .await
        {
            error!(error = ?err, "Failed to process inbound metadata stream");
        }
    }
}

// Coalesces concurrent inbound reconcile triggers: one run in flight, all
// further triggers fold their topic sets into a single queued re-run.
#[derive(Debug, Default)]
struct DocumentReconcileCoalescer {
    state: Mutex<DocumentReconcileQueue>,
    shutdown: Shutdown,
}

#[derive(Debug, Default)]
struct DocumentReconcileQueue {
    running: bool,
    queued: BTreeSet<irokle::TopicId>,
    queued_since: Option<Instant>,
}

impl DocumentReconcileCoalescer {
    fn new(shutdown: Shutdown) -> Self {
        Self {
            state: Mutex::default(),
            shutdown,
        }
    }

    fn trigger(self: &Arc<Self>, context: Arc<DriverContext>, topics: Vec<irokle::TopicId>) {
        // Reconcile runs write metadata and storage: none may start once
        // shutdown has begun draining.
        if self.shutdown.is_triggered() {
            return;
        }
        {
            let mut state = self.state.lock().unwrap_or_else(|lock| lock.into_inner());
            state.queued.extend(topics);
            if !state.queued.is_empty() && state.queued_since.is_none() {
                state.queued_since = Some(Instant::now());
            }
            if state.running || state.queued.is_empty() {
                return;
            }
            state.running = true;
        }
        let coalescer = self.clone();
        self.shutdown.spawn(async move {
            let mut failures = 0u32;
            loop {
                let batch: Vec<irokle::TopicId> = {
                    let mut state = coalescer
                        .state
                        .lock()
                        .unwrap_or_else(|lock| lock.into_inner());
                    if state.queued.is_empty() {
                        state.running = false;
                        state.queued_since = None;
                        return;
                    }
                    state.queued_since = None;
                    std::mem::take(&mut state.queued).into_iter().collect()
                };
                if reconcile_inbound_topics(&context, batch.clone()).await {
                    failures = 0;
                } else {
                    coalescer.trigger(context.clone(), batch);
                    let retry_after = Duration::from_millis(retry_delay_ms(failures));
                    failures = failures.saturating_add(1);
                    sleep(retry_after).await;
                }
            }
        });
    }

    fn trigger_all(self: &Arc<Self>, context: Arc<DriverContext>) {
        let Some(net_handle) = context.net_handle.as_ref() else {
            return;
        };
        let Ok(topics) = net_handle.document_sync_node().list_topics() else {
            return;
        };
        self.trigger(
            context,
            topics.into_iter().map(|topic| topic.topic_id).collect(),
        );
    }

    fn lag_snapshot(&self) -> (usize, bool, u64) {
        let state = self.state.lock().unwrap_or_else(|lock| lock.into_inner());
        let oldest_age_ms = state
            .queued_since
            .map(|since| duration_ms(since.elapsed()))
            .unwrap_or(0);
        (state.queued.len(), state.running, oldest_age_ms)
    }
}

// Emits a `queue.lag` line every tick while the coalescer holds queued topics
// or a reconcile run is in flight, plus one final line once it drains.
fn spawn_queue_gauge(coalescer: Weak<DocumentReconcileCoalescer>, shutdown: &Shutdown) {
    if tokio::runtime::Handle::try_current().is_err() {
        return;
    }
    let cancelled = shutdown.token();
    shutdown.spawn(async move {
        let mut was_active = false;
        loop {
            tokio::select! {
                _ = cancelled.cancelled() => return,
                _ = sleep(QUEUE_LAG_INTERVAL) => {}
            }
            let Some(coalescer) = coalescer.upgrade() else {
                return;
            };
            let (depth, running, oldest_age_ms) = coalescer.lag_snapshot();
            let active = depth > 0 || running;
            if active || was_active {
                info!(
                    event = "queue.lag",
                    queue = "reconcile_coalesce",
                    depth,
                    running,
                    oldest_age_ms,
                    "Inbound reconcile coalescing queue lag"
                );
            }
            was_active = active;
        }
    });
}

async fn reconcile_inbound_topics(
    context: &Arc<DriverContext>,
    topics: Vec<irokle::TopicId>,
) -> bool {
    let Some(net_handle) = context.net_handle.clone() else {
        return true;
    };
    let run_started = Instant::now();
    let topic_count = topics.len();
    let targets = match net_handle.reconcile_sync_topics(topics).await {
        Ok(targets) => targets,
        Err(err) => {
            error!(error = ?err, "Failed to reconcile inbound document sync topics");
            return false;
        }
    };
    let reconcile_elapsed = run_started.elapsed();
    let applied = targets.applied();
    debug!(applied, "Reconciled inbound document sync events");
    if applied == 0 {
        return true;
    }
    let dashboard_changed = targets_change_dashboard(&targets.targets);
    let metadata_graph_tombstones = targets.metadata_graph_tombstones.clone();
    let realm_config_changed = targets
        .targets
        .iter()
        .any(|target| matches!(target, DocumentTarget::RealmConfig { .. }));
    if realm_config_changed {
        // Transition-free: a heavy transition step here would stall document
        // application; the reconcile arms the SyncPlacements timer instead.
        reconcile_shard_topics(context, *net_handle.realm_id(), net_handle.node_id()).await;
        let (realm_id, node_id) = (*net_handle.realm_id(), net_handle.node_id());
        crate::federation::publish::publish_soon(context, realm_id, node_id).await;
    }
    refresh_usage_targets(context, net_handle.node_id(), &targets.targets).await;
    crate::notifications::email::config_applied(context, &targets.targets).await;
    // An installed record may name a member this node bootstraps.
    wake_bootstrap(context).await;
    refresh_target_interest(context, &targets.targets).await;
    let project_started = Instant::now();
    project_inbound_events(context, targets).await;
    let project_elapsed = project_started.elapsed();
    let prune_started = Instant::now();
    process_graph_tombstones(context, metadata_graph_tombstones).await;
    info!(
        event = "pipeline.reconcile.summary",
        topics = topic_count,
        applied,
        reconcile_ms = duration_ms(reconcile_elapsed),
        project_ms = duration_ms(project_elapsed),
        prune_ms = duration_ms(prune_started.elapsed()),
        total_ms = duration_ms(run_started.elapsed()),
        "Inbound document sync reconcile summary"
    );
    if dashboard_changed {
        notify_dashboard_change(context);
    }
    true
}

/// Lifecycle owner for a test-installed inbound handler: it holds the job
/// runtime and shutdown the handler runs under, so cleanup can deregister the
/// handler and drain its maintenance children before storage closes.
pub struct IncomingFixture {
    shutdown: Shutdown,
    jobs_runtime: Arc<JobsRuntime>,
    net_handle: aruna_net::NetHandle,
    handler: Weak<OperationsInboundHandler>,
}

impl IncomingFixture {
    /// Returns the job runtime the installed handler was given.
    pub fn jobs_runtime(&self) -> Arc<JobsRuntime> {
        self.jobs_runtime.clone()
    }

    /// Returns the shutdown tracking the handler's background children.
    pub fn shutdown(&self) -> &Shutdown {
        &self.shutdown
    }

    /// Whether the handler is still registered on its net handle.
    pub fn installed(&self) -> bool {
        self.handler.strong_count() > 0
    }

    /// Deregisters the handler, stops job admission, and drains the tracked
    /// children; reports whether every child stopped within `timeout`.
    pub async fn stop(&self, timeout: Duration) -> bool {
        self.net_handle.clear_inbound_handler();
        self.jobs_runtime.close_admission();
        self.shutdown.drain(timeout).await
    }
}

/// Installs a handler under fixture-owned lifecycle owners; bind the fixture
/// and call [`IncomingFixture::stop`] to deregister and drain before dropping
/// test storage. Startup uses [`initialize_net_holder`] with the node owners.
pub fn initialize_incoming_fixture(context: Arc<DriverContext>) -> Option<IncomingFixture> {
    let net_handle = context.net_handle.clone()?;
    let shutdown = Shutdown::new();
    let jobs_runtime = JobsRuntime::new();
    let handler = install_inbound_handler(
        context,
        RoCrateLimits::default(),
        jobs_runtime.clone(),
        &shutdown,
    )?;
    Some(IncomingFixture {
        shutdown,
        jobs_runtime,
        net_handle,
        handler: Arc::downgrade(&handler),
    })
}

pub fn initialize_net_holder(
    context: Arc<DriverContext>,
    rocrate_limits: RoCrateLimits,
    jobs_runtime: Arc<JobsRuntime>,
    shutdown: &Shutdown,
) {
    install_inbound_handler(context, rocrate_limits, jobs_runtime, shutdown);
}

fn install_inbound_handler(
    context: Arc<DriverContext>,
    rocrate_limits: RoCrateLimits,
    jobs_runtime: Arc<JobsRuntime>,
    shutdown: &Shutdown,
) -> Option<Arc<OperationsInboundHandler>> {
    let Some(net_handle) = context.net_handle.clone() else {
        warn!("Cannot initialize inbound handling without net handle");
        return None;
    };
    let metadata_handle = context.metadata_handle.clone();
    let inbound_handler = Arc::new(OperationsInboundHandler::new(
        context.clone(),
        rocrate_limits,
        jobs_runtime,
        shutdown.clone(),
    ));

    net_handle.set_inbound_handler(inbound_handler.clone());
    if let Some(metadata_handle) = metadata_handle {
        schedule_sync_maintenance(
            context,
            Arc::downgrade(&inbound_handler.document_sync_reconcile),
            metadata_handle,
            shutdown,
        );
    }
    Some(inbound_handler)
}

#[async_trait]
impl InboundEventHandler for OperationsInboundHandler {
    #[tracing::instrument(
        name = "operations.inbound.stream",
        level = "debug",
        skip(self, stream),
        fields(peer = %node_id, alpn = ?alpn)
    )]
    async fn handle_incoming_stream(&self, alpn: Alpn, stream: BiStream, node_id: NodeId) {
        let span = info_span!("net.incoming_stream", peer = %node_id, alpn = ?alpn);

        async move {
            trace!(event = "stream.received", peer = %node_id, alpn = ?alpn, "Received inbound stream");
            // Debug poll frames reserve stack for every branch at once, so each
            // protocol branch is boxed to keep only the active one on the stack.
            match alpn {
                Alpn::Bao => {
                    Box::pin(crate::replication::incoming::stream::handle_bao_stream(
                        self.context.as_ref(),
                        &self.rocrate_limits,
                        stream,
                        node_id,
                    ))
                    .await;
                }
                Alpn::DocumentSync => {
                    Box::pin(self.accept_document_sync(stream, node_id)).await;
                }
                Alpn::Metadata => {
                    Box::pin(self.handle_metadata_stream(stream, node_id)).await;
                }
                Alpn::NativeReference => {
                    Box::pin(crate::staging::native_source::handle_native_stream(
                        self.context.as_ref(),
                        stream,
                        node_id,
                    ))
                    .await;
                }
                Alpn::Notification => {
                    Box::pin(async {
                        crate::notifications::incoming::handle_notification_stream(
                            self.context.as_ref(),
                            stream,
                            node_id,
                        )
                        .await;
                    })
                    .await
                }
                Alpn::Shard => {
                    Box::pin(async {
                        crate::shard::incoming::handle_shard_stream(
                            self.context.as_ref(),
                            stream,
                            node_id,
                        )
                        .await;
                    })
                    .await
                }
                Alpn::Heartbeat => {
                    Box::pin(crate::node::heartbeat::handle_heartbeat_stream(
                        self.context.as_ref(),
                        stream,
                        node_id,
                    ))
                    .await
                }
                Alpn::JobControl => {
                    Box::pin(async {
                        crate::jobs::protocol::handle_job_stream(
                            self.context.as_ref(),
                            &self.jobs_runtime,
                            stream,
                            node_id,
                        )
                        .await;
                    })
                    .await
                }
                Alpn::Dht => {
                    warn!(
                        peer = %node_id,
                        "Ignoring inbound stream for non-stream ALPN"
                    );
                }
            }
        }
        .instrument(span)
        .await;
    }

    async fn handle_evicted_documents(&self, documents: Vec<DocumentEvictedDocument>) -> bool {
        reemit_evicted_documents(self.context.as_ref(), documents).await
    }
}

/// Re-enqueues payloads from a genesis tie-break eviction as outbox records replaying
/// onto the winning chain; the evicted event id makes this idempotent, and no genesis
/// is minted. Returns whether every record is durable; otherwise the journal is kept.
async fn reemit_evicted_documents(
    context: &DriverContext,
    documents: Vec<DocumentEvictedDocument>,
) -> bool {
    let Some(net_handle) = context.net_handle.as_ref() else {
        warn!(task_id = ?TaskKey::DrainSyncOutbox, "Cannot re-emit evicted documents without net handle");
        return false;
    };
    let node_id = net_handle.node_id();
    let mut written = 0usize;
    let mut complete = true;
    for document in documents {
        let record = new_identified_record(
            document.event_id,
            node_id,
            document.target,
            Vec::new(),
            document.event,
            document.placement,
            document.allow_genesis,
        );
        let effect = match write_outbox_effect(&record) {
            Ok(effect) => effect,
            Err(error) => {
                warn!(task_id = ?TaskKey::DrainSyncOutbox, error = %error, "Failed to encode re-emitted eviction outbox record");
                complete = false;
                continue;
            }
        };
        match context.storage_handle.send_effect(effect).await {
            Event::Storage(StorageEvent::WriteResult { .. }) => written += 1,
            Event::Storage(StorageEvent::Error { error }) => {
                warn!(task_id = ?TaskKey::DrainSyncOutbox, error = %error, "Failed to write re-emitted eviction outbox record");
                complete = false;
            }
            other => {
                warn!(task_id = ?TaskKey::DrainSyncOutbox, event = ?other, "Unexpected event writing re-emitted eviction outbox record");
                complete = false;
            }
        }
    }
    if written == 0 {
        return complete;
    }
    let Some(task_handle) = context.task_handle.as_ref() else {
        warn!(task_id = ?TaskKey::DrainSyncOutbox, "Cannot schedule outbox drain for re-emitted evictions without task handle");
        return complete;
    };
    if let Event::Task(TaskEvent::Error { message, .. }) =
        task_handle.send_effect(schedule_drain_effect()).await
    {
        warn!(task_id = ?TaskKey::DrainSyncOutbox, message = %message, "Failed to schedule outbox drain after re-emitting evictions");
    }
    info!(
        count = written,
        "Re-emitted evicted documents to the sync outbox"
    );
    complete
}

async fn project_inbound_events(context: &DriverContext, reconciled: DocumentReconcileResult) {
    if !reconciled.metadata_create_events.is_empty() {
        let local_node_id = context.net_handle.as_ref().map(|net| net.node_id());
        if let Err(error) =
            project_create_events(context, reconciled.metadata_create_events, local_node_id).await
        {
            error!(
                error = ?error,
                "Failed to project metadata create event batch after inbound document sync reconciliation"
            );
            schedule_projection_retry(context).await;
        }
        return;
    }

    let mut targets = Vec::new();
    for target in reconciled.targets {
        let DocumentTarget::MetadataCreateEvent {
            document_id,
            event_id,
            ..
        } = target
        else {
            continue;
        };
        targets.push((document_id, event_id));
    }
    if let Err(error) = project_logged_events(context, targets).await {
        error!(
            error = ?error,
            "Failed to project metadata create event batch from log after inbound document sync reconciliation"
        );
        schedule_projection_retry(context).await;
    }
}

async fn schedule_projection_retry(context: &DriverContext) {
    if let Err(error) = schedule_projection_drain(context, PROJECTION_RETRY_AFTER).await {
        warn!(task_id = ?TaskKey::DrainProjectionQueue, error = ?error, "Failed to schedule metadata projection retry");
    }
}

fn schedule_sync_maintenance(
    context: Arc<DriverContext>,
    coalescer: Weak<DocumentReconcileCoalescer>,
    metadata_handle: MetadataHandle,
    shutdown: &Shutdown,
) {
    let jitter = Duration::from_secs(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|now| now.subsec_nanos() as u64 % MAINTENANCE_JITTER_SECS)
            .unwrap_or(0),
    );
    // This loop writes the metadata store, so it has to stop before the
    // shutdown flushes it.
    let cancelled = shutdown.token();
    shutdown.spawn(async move {
        let mut cycle = 0usize;
        loop {
            tokio::select! {
                _ = cancelled.cancelled() => return,
                _ = sleep(SYNC_MAINTENANCE_INTERVAL + jitter) => {}
            }
            let Some(coalescer) = coalescer.upgrade() else {
                return;
            };
            coalescer.trigger_all(context.clone());
            cycle = cycle.saturating_add(1);
            run_sync_maintenance(&metadata_handle, "periodic", cycle).await;
        }
    });
}

async fn run_sync_maintenance(
    metadata_handle: &MetadataHandle,
    source: &'static str,
    attempt: usize,
) {
    if let Err(error) = metadata_handle.reconcile_document_sync().await {
        warn!(
            source,
            attempt,
            error = ?error,
            "Metadata document sync reconciliation failed"
        );
    }
    match metadata_handle.prune_deleted_graphs().await {
        Ok(pruned) if pruned > 0 => {
            debug!(source, attempt, pruned, "Metadata graph prune completed")
        }
        Ok(_) => {}
        Err(error) => warn!(
            source,
            attempt,
            error = ?error,
            "Metadata graph prune failed"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::protocol::LocationSummaryRequest;
    use aruna_blob::blob::BlobHandler;
    use aruna_core::UserId;
    use aruna_core::events::StorageEvent;
    use aruna_core::keyspaces::{S3_BUCKET_KEYSPACE, TASK_TIMER_KEYSPACE};
    use aruna_core::structs::execution::source_access::SourceMetadata;
    use aruna_core::structs::execution::source_connector::SourceConnectorKind;
    use aruna_core::structs::execution::staging::{
        PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
    };
    use aruna_core::structs::identity::auth::PathRestriction;
    use aruna_core::structs::storage::blob::{Backend, BackendConfig, BucketInfo};
    use aruna_core::structs::storage::format::Compression;
    use aruna_core::structs::storage::replication_queue::ReferenceAdvance;
    use aruna_core::task::{PersistedTaskTimer, TaskKey};
    use aruna_net::{DiscoveryMethod, NetConfig, RelayMethod};
    use aruna_storage::FjallStorage;
    use aruna_tasks::TaskHandle;
    use std::collections::HashMap;
    use tempfile::tempdir;
    use tokio::io::AsyncWriteExt;
    use tokio::sync::mpsc;

    #[derive(Debug)]
    struct StreamCapture(mpsc::UnboundedSender<(Alpn, BiStream, NodeId)>);

    #[async_trait]
    impl InboundEventHandler for StreamCapture {
        async fn handle_incoming_stream(&self, alpn: Alpn, stream: BiStream, node_id: NodeId) {
            self.0.send((alpn, stream, node_id)).unwrap();
        }
    }

    /// One evicted delete of a lifecycle document, re-emitted under its own event id.
    fn evicted(seed: u8) -> DocumentEvictedDocument {
        let event_id = Ulid::from_parts(u64::from(seed), u128::from(seed));
        let placement = aruna_core::structs::placement::record::PlacementRef::NIL;
        let change = aruna_core::document::DocumentChange {
            base: None,
            current: aruna_core::document::DocumentSyncRevision {
                generation: 1,
                event_id,
                actor: iroh::SecretKey::from_bytes(&[seed; 32]).public(),
                updated_at_ms: u64::from(seed),
            },
            kind: aruna_core::document::DocumentChangeKind::Delete,
            placement,
        };
        DocumentEvictedDocument {
            event_id,
            target: DocumentTarget::MetadataDocumentLifecycle {
                document_id: event_id,
            },
            event: aruna_core::document::DocumentOutboxEvent::Delete { change },
            placement,
            allow_genesis: false,
        }
    }

    #[tokio::test]
    async fn reemit_lands_once() {
        // A failed first write reports the hand-off incomplete, so the journal keeps it; after
        // recovery every replay writes each document once, under its evicted event id.
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().join("storage").to_str().unwrap()).unwrap();
        let net_handle = aruna_net::NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                sync_storage_path: Some(dir.path().join("document-sync")),
                ..NetConfig::default()
            },
            storage.clone(),
        )
        .await
        .unwrap();
        let context = |storage_handle| DriverContext {
            storage_handle,
            net_handle: Some(net_handle.clone()),
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let documents = vec![evicted(1), evicted(2)];
        let (failed, receivers) = aruna_storage::StorageHandle::new();
        drop(receivers);
        let failed = context(failed);
        assert!(!reemit_evicted_documents(&failed, documents.clone()).await);

        let recovered = context(storage.clone());
        for _ in 0..2 {
            assert!(reemit_evicted_documents(&recovered, documents.clone()).await);
        }
        let rows = match storage
            .send_storage_effect(aruna_core::effects::StorageEffect::Iter {
                key_space: aruna_core::keyspaces::SYNC_OUTBOX_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 16,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values,
            other => panic!("unexpected outbox read: {other:?}"),
        };
        let mut ids: Vec<_> = rows
            .iter()
            .map(|(_, value)| {
                postcard::from_bytes::<aruna_core::document::DocumentOutboxRecord>(value)
                    .unwrap()
                    .outbox_id
            })
            .collect();
        ids.sort();
        let expected: Vec<_> = documents.iter().map(|document| document.event_id).collect();
        assert_eq!(ids, expected);
        net_handle.shutdown().await;
    }

    // A replica into an offered bucket, delete markers included, is denied, so the sender stops.
    #[tokio::test]
    async fn failed_bao_closes() {
        let dir_a = tempdir().unwrap();
        let dir_b = tempdir().unwrap();
        let storage_a = FjallStorage::open(dir_a.path().to_str().unwrap()).unwrap();
        let storage_b = FjallStorage::open(dir_b.path().to_str().unwrap()).unwrap();
        let config = || NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        };
        let net_a = aruna_net::NetHandle::new(config(), storage_a)
            .await
            .unwrap();
        let net_b = aruna_net::NetHandle::new(config(), storage_b.clone())
            .await
            .unwrap();
        net_a.add_peer_addr(net_b.endpoint_addr()).await;
        net_b.add_peer_addr(net_a.endpoint_addr()).await;

        let (stream_tx, mut stream_rx) = mpsc::unbounded_channel();
        net_b.set_inbound_handler(Arc::new(StreamCapture(stream_tx)));
        let blob_root = dir_b.path().join("blobs");
        std::fs::create_dir(&blob_root).unwrap();
        let blob_handle = BlobHandler::new(
            BackendConfig {
                backend_type: Backend::FileSystem,
                root: blob_root.to_str().unwrap().to_string(),
                service_config: HashMap::new(),
                bucket_prefix: None,
                max_bucket_size: None,
                multipart_bucket: None,
                timeouts: Default::default(),
            },
            storage_b.clone(),
            net_b.clone(),
        )
        .await
        .unwrap();
        let handler = OperationsInboundHandler::new(
            Arc::new(DriverContext {
                storage_handle: storage_b,
                net_handle: Some(net_b.clone()),
                blob_handle: Some(blob_handle),
                metadata_handle: None,
                task_handle: None,
                compute_handle: None,
            }),
            RoCrateLimits::default(),
            JobsRuntime::new(),
            Shutdown::new(),
        );

        let mut outbound = net_a.open_stream(net_b.node_id(), Alpn::Bao).await.unwrap();
        outbound.0.write_u32(8).await.unwrap();
        outbound.0.write_all(b"x").await.unwrap();
        outbound.0.finish().unwrap();
        let (alpn, inbound, peer) = tokio::time::timeout(Duration::from_secs(5), stream_rx.recv())
            .await
            .unwrap()
            .unwrap();

        handler.handle_incoming_stream(alpn, inbound, peer).await;

        let closed =
            tokio::time::timeout(Duration::from_millis(250), outbound.1.read_to_end(1)).await;
        assert!(
            matches!(closed, Ok(Ok(ref bytes)) if bytes.is_empty()),
            "BR-003_EXPECT_CLOSE: failed initial Bao stream remained open: {closed:?}"
        );

        net_a.shutdown().await;
        net_b.shutdown().await;
    }

    #[tokio::test]
    async fn fixtures_stop_cleanly() {
        for _ in 0..2 {
            let dir = tempdir().unwrap();
            let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
            let net = aruna_net::NetHandle::new(
                NetConfig {
                    bind_addr: "127.0.0.1:0".parse().unwrap(),
                    discovery_method: DiscoveryMethod::None,
                    relay_method: RelayMethod::None,
                    sync_storage_path: Some(dir.path().join("document-sync")),
                    ..NetConfig::default()
                },
                storage.clone(),
            )
            .await
            .unwrap();
            let metadata_handle = MetadataHandle::new(
                dir.path().join("metadata"),
                net.node_id(),
                storage.clone(),
                Some(net.clone()),
                Some(net.document_sync_node()),
                Some(net.document_sync_database()),
            )
            .unwrap();
            let context = Arc::new(DriverContext {
                storage_handle: storage,
                net_handle: Some(net.clone()),
                blob_handle: None,
                metadata_handle: Some(metadata_handle),
                task_handle: None,
                compute_handle: None,
            });

            let fixture =
                initialize_incoming_fixture(context.clone()).expect("net handle installs");
            assert!(fixture.installed(), "handler registered on the net handle");
            // The gauge and the periodic maintenance loop are both tracked.
            assert!(fixture.shutdown().tracked_children() >= 2);
            assert!(
                fixture.stop(Duration::from_secs(5)).await,
                "maintenance children drained"
            );
            assert_eq!(fixture.shutdown().tracked_children(), 0);
            assert!(!fixture.installed(), "handler deregistered");

            drop(fixture);
            net.shutdown().await;
            drop(context);
        }
    }

    #[tokio::test]
    async fn projection_failure_retries() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
        };
        let document_id = ulid::Ulid::generate();
        let event_id = ulid::Ulid::generate();

        project_inbound_events(
            &context,
            DocumentReconcileResult {
                targets: vec![DocumentTarget::MetadataCreateEvent {
                    document_id,
                    event_id,
                }],
                metadata_create_events: Vec::new(),
                metadata_graph_tombstones: Vec::new(),
            },
        )
        .await;

        let timer = read_task_timer(&storage, &TaskKey::DrainProjectionQueue)
            .await
            .expect("projection retry timer persisted");
        assert_eq!(timer.key, TaskKey::DrainProjectionQueue);
    }

    async fn read_task_timer(
        storage: &aruna_storage::StorageHandle,
        key: &TaskKey,
    ) -> Option<PersistedTaskTimer> {
        let event = storage
            .send_storage_effect(aruna_core::effects::StorageEffect::Read {
                key_space: TASK_TIMER_KEYSPACE.to_string(),
                key: postcard::to_allocvec(key).unwrap().into(),
                txn_id: None,
            })
            .await;
        match event {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                value.map(|value| postcard::from_bytes(&value).expect("timer decodes"))
            }
            other => panic!("unexpected task timer read event: {other:?}"),
        }
    }

    /// Two joined nodes with net_b's realm config in storage, so net_a can be
    /// admitted as infrastructure or as a device. `exchange` pushes one framed
    /// control message through the real handler and reports stream closure.
    struct BaoBoundary {
        net_a: aruna_net::NetHandle,
        net_b: aruna_net::NetHandle,
        handler: OperationsInboundHandler,
        inbound: mpsc::UnboundedReceiver<(Alpn, BiStream, NodeId)>,
        _dirs: (tempfile::TempDir, tempfile::TempDir),
    }

    impl BaoBoundary {
        async fn start(device_peer: bool) -> Self {
            let dir_a = tempdir().unwrap();
            let dir_b = tempdir().unwrap();
            let storage_a = FjallStorage::open(dir_a.path().to_str().unwrap()).unwrap();
            let storage_b = FjallStorage::open(dir_b.path().to_str().unwrap()).unwrap();
            let realm_id = RealmId::from_bytes([8u8; 32]);
            let config = |realm_id: RealmId| NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                realm_id,
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..NetConfig::default()
            };
            let net_a = aruna_net::NetHandle::new(config(realm_id), storage_a)
                .await
                .unwrap();
            let net_b = aruna_net::NetHandle::new(config(realm_id), storage_b.clone())
                .await
                .unwrap();
            net_a.add_peer_addr(net_b.endpoint_addr()).await;
            net_b.add_peer_addr(net_a.endpoint_addr()).await;

            let mut realm =
                aruna_core::structs::identity::realm::RealmConfigDocument::default_for_realm(
                    realm_id,
                    Vec::new(),
                );
            realm.ensure_node(
                net_b.node_id(),
                aruna_core::structs::identity::realm::RealmNodeKind::Server,
            );
            let owner = UserId::nil(realm_id);
            if device_peer {
                realm.ensure_node(
                    net_a.node_id(),
                    aruna_core::structs::identity::realm::RealmNodeKind::User { owner },
                );
            } else {
                realm.ensure_node(
                    net_a.node_id(),
                    aruna_core::structs::identity::realm::RealmNodeKind::Server,
                );
            }
            let actor = aruna_core::structs::identity::auth::Actor {
                node_id: net_b.node_id(),
                user_id: owner,
                realm_id,
            };
            let realm_target = DocumentTarget::RealmConfig { realm_id };
            storage_b
                .send_storage_effect(aruna_core::effects::StorageEffect::Write {
                    key_space: realm_target.storage_keyspace().to_string(),
                    key: realm_target.storage_key(),
                    value: realm.to_bytes(&actor).unwrap().into(),
                    txn_id: None,
                })
                .await;

            let (stream_tx, stream_rx) = mpsc::unbounded_channel();
            net_b.set_inbound_handler(Arc::new(StreamCapture(stream_tx)));
            let blob_root = dir_b.path().join("blobs");
            std::fs::create_dir(&blob_root).unwrap();
            let blob_handle = BlobHandler::new(
                BackendConfig {
                    backend_type: Backend::FileSystem,
                    root: blob_root.to_str().unwrap().to_string(),
                    service_config: HashMap::new(),
                    bucket_prefix: None,
                    max_bucket_size: None,
                    multipart_bucket: None,
                    timeouts: Default::default(),
                },
                storage_b.clone(),
                net_b.clone(),
            )
            .await
            .unwrap();
            let handler = OperationsInboundHandler::new(
                Arc::new(DriverContext {
                    storage_handle: storage_b,
                    net_handle: Some(net_b.clone()),
                    blob_handle: Some(blob_handle),
                    metadata_handle: None,
                    task_handle: None,
                    compute_handle: None,
                }),
                RoCrateLimits::default(),
                JobsRuntime::new(),
                Shutdown::new(),
            );
            Self {
                net_a,
                net_b,
                handler,
                inbound: stream_rx,
                _dirs: (dir_a, dir_b),
            }
        }

        async fn exchange(&mut self, frame: &[u8]) -> bool {
            let mut outbound = self
                .net_a
                .open_stream(self.net_b.node_id(), Alpn::Bao)
                .await
                .unwrap();
            outbound.0.write_all(frame).await.unwrap();
            outbound.0.finish().unwrap();
            let (alpn, stream, peer) =
                tokio::time::timeout(Duration::from_secs(5), self.inbound.recv())
                    .await
                    .expect("inbound stream captured")
                    .expect("inbound stream channel open");
            self.handler
                .handle_incoming_stream(alpn, stream, peer)
                .await;
            let closed =
                tokio::time::timeout(Duration::from_millis(250), outbound.1.read_to_end(1)).await;
            matches!(closed, Ok(Ok(ref bytes)) if bytes.is_empty())
        }

        async fn reply(&mut self, frame: &[u8]) -> Vec<u8> {
            let mut outbound = self
                .net_a
                .open_stream(self.net_b.node_id(), Alpn::Bao)
                .await
                .unwrap();
            outbound.0.write_all(frame).await.unwrap();
            outbound.0.finish().unwrap();
            let (alpn, stream, peer) =
                tokio::time::timeout(Duration::from_secs(5), self.inbound.recv())
                    .await
                    .expect("inbound stream captured")
                    .expect("inbound stream channel open");
            self.handler
                .handle_incoming_stream(alpn, stream, peer)
                .await;
            let reply = tokio::time::timeout(
                Duration::from_millis(250),
                outbound.1.read_to_end(64 * 1024),
            )
            .await
            .expect("protocol reply arrived")
            .expect("reply stream readable");
            assert!(
                !reply.is_empty(),
                "BR-003_EXPECT_REPLY: routed message closed without a protocol reply"
            );
            reply
        }
    }

    fn replication_frame(message: &VersionReplicationMessage) -> Vec<u8> {
        let payload = message.to_bytes().expect("replication message encodes");
        let mut frame = (payload.len() as u32).to_be_bytes().to_vec();
        frame.extend_from_slice(&payload);
        frame
    }

    #[tokio::test]
    async fn refuses_device_family() {
        let mut boundary = BaoBoundary::start(true).await;
        let manifest = VersionReplicationMessage::VersionManifest(VersionReplicationManifest {
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            version_id: Ulid::generate(),
            group_id: Ulid::generate(),
            kind: ReplicationItemKind::Materialized,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: UserId::nil(RealmId::from_bytes([8u8; 32])),
            current_version: true,
            current_version_generation: None,
            auth_context: AuthContext::anonymous(RealmId::from_bytes([8u8; 32])),
            blob: None,
            source: None,
            multipart: None,
            reference_intent: false,
            origin: None,
            upstream_sources: Vec::new(),
            writer_auth_context: None,
            reference_metadata: None,
            metadata: HashMap::new(),
            reference_advance: None,
            reference_advance_count: None,
            placement_policies: Vec::new(),
            original_epoch: Default::default(),
            version_generation: 0,
            logical_identity: None,
            distribution: None,
            version_origin: None,
            version_hop: None,
            sync_source: None,
        });
        let summary = VersionReplicationMessage::LocationSummaryRequest(LocationSummaryRequest {
            realm_id: RealmId::from_bytes([8u8; 32]),
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            version_id: None,
            auth_context: AuthContext::anonymous(RealmId::from_bytes([8u8; 32])),
        });
        for message in [manifest, summary] {
            assert!(
                boundary.exchange(&replication_frame(&message)).await,
                "BR-003_EXPECT_CLOSE: device bao family refusal remained open"
            );
        }
        boundary.net_a.shutdown().await;
        boundary.net_b.shutdown().await;
    }

    #[tokio::test]
    async fn bao_frame_validation() {
        let mut boundary = BaoBoundary::start(false).await;
        let mut malformed = 8u32.to_be_bytes().to_vec();
        malformed.extend_from_slice(b"x");
        let oversized = u32::MAX.to_be_bytes().to_vec();
        for frame in [malformed, oversized] {
            assert!(
                boundary.exchange(&frame).await,
                "BR-003_EXPECT_CLOSE: malformed bao frame remained open"
            );
        }
        boundary.net_a.shutdown().await;
        boundary.net_b.shutdown().await;
    }

    #[tokio::test]
    async fn answers_location_summary() {
        let mut boundary = BaoBoundary::start(false).await;
        let realm_id = RealmId::from_bytes([8u8; 32]);
        // The bucket record names a group with no authorization document in
        // storage, so the operation answers a deterministic typed denial.
        let bucket = BucketInfo {
            group_id: Ulid::generate(),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: UserId::nil(realm_id),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        };
        boundary
            .handler
            .context
            .storage_handle
            .send_storage_effect(aruna_core::effects::StorageEffect::Write {
                key_space: S3_BUCKET_KEYSPACE.to_string(),
                key: b"bucket".to_vec().into(),
                value: bucket.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
        let request = VersionReplicationMessage::LocationSummaryRequest(LocationSummaryRequest {
            realm_id,
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            version_id: None,
            auth_context: AuthContext::anonymous(realm_id),
        });
        let reply = boundary.reply(&replication_frame(&request)).await;
        let len = u32::from_be_bytes(reply[..4].try_into().unwrap()) as usize;
        assert_eq!(len, reply.len() - 4, "reply frame length matches payload");
        assert_eq!(
            VersionReplicationMessage::from_bytes(&reply[4..]).unwrap(),
            VersionReplicationMessage::LocationSummaryDenied,
            "BR-003_EXPECT_REPLY: an accepted message must answer, not close silently"
        );
        boundary.net_a.shutdown().await;
        boundary.net_b.shutdown().await;
    }

    #[tokio::test]
    async fn cancel_drops_handler() {
        let mut boundary = BaoBoundary::start(false).await;
        let mut outbound = boundary
            .net_a
            .open_stream(boundary.net_b.node_id(), Alpn::Bao)
            .await
            .unwrap();
        // One byte surfaces the stream without completing a frame, so the
        // handler is inside its bounded read when the outer timeout drops it.
        outbound.0.write_all(&[0u8]).await.unwrap();
        let (alpn, stream, peer) =
            tokio::time::timeout(Duration::from_secs(5), boundary.inbound.recv())
                .await
                .expect("inbound stream captured")
                .expect("inbound stream channel open");
        let cancelled = tokio::time::timeout(
            Duration::from_millis(200),
            boundary.handler.handle_incoming_stream(alpn, stream, peer),
        )
        .await;
        assert!(cancelled.is_err(), "handler completed without a frame");
        drop(outbound);
        boundary.net_a.shutdown().await;
        boundary.net_b.shutdown().await;
    }
}
