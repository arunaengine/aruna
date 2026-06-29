use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use crate::driver::{DriverContext, drive};
use crate::metadata::MetadataHandle;
use crate::metadata::projector::{
    METADATA_PROJECTION_RETRY_AFTER, project_metadata_create_events,
    project_metadata_create_events_from_log, schedule_pending_metadata_projection_drain,
};
use crate::metadata::prune_queue::process_metadata_graph_tombstones;
use crate::process_placements::{PlacementConfig, ProcessPlacementsOperation};
use crate::replication::incoming_version_replication::IncomingVersionReplicationOperation;
use crate::replication::protocol::VersionReplicationMessage;
use aruna_core::alpn::Alpn;
use aruna_core::document::{DocumentSyncReconcileResult, DocumentSyncTarget};
use aruna_core::effects::BlobEffect;
use aruna_core::events::{BlobEvent, Event};
use aruna_core::id::NodeId;
use aruna_core::telemetry::{QUEUE_LAG_INTERVAL, duration_ms};
use aruna_net::InboundEventHandler;
use aruna_net::streams::BiStream;
use async_trait::async_trait;
use tokio::time::sleep;
use tracing::{Instrument, debug, error, info, info_span, trace, warn};

const METADATA_DOCUMENT_SYNC_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(60);
const METADATA_DOCUMENT_SYNC_MAINTENANCE_JITTER_SECS: u64 = 15;

#[derive(Debug)]
struct OperationsInboundHandler {
    context: Arc<DriverContext>,
    document_sync_reconcile: Arc<DocumentSyncReconcileCoalescer>,
}

impl OperationsInboundHandler {
    fn new(context: Arc<DriverContext>) -> Self {
        let document_sync_reconcile = Arc::new(DocumentSyncReconcileCoalescer::default());
        spawn_reconcile_queue_gauge(Arc::downgrade(&document_sync_reconcile));
        Self {
            context,
            document_sync_reconcile,
        }
    }
}

// Coalesces concurrent inbound reconcile triggers: one run in flight, all
// further triggers fold their topic sets into a single queued re-run.
#[derive(Debug, Default)]
struct DocumentSyncReconcileCoalescer {
    state: Mutex<DocumentSyncReconcileQueue>,
}

#[derive(Debug, Default)]
struct DocumentSyncReconcileQueue {
    running: bool,
    queued: BTreeSet<irokle::TopicId>,
    queued_since: Option<Instant>,
}

impl DocumentSyncReconcileCoalescer {
    fn trigger(self: &Arc<Self>, context: Arc<DriverContext>, topics: Vec<irokle::TopicId>) {
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
        tokio::spawn(async move {
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
                reconcile_inbound_document_sync_topics(&context, batch).await;
            }
        });
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
fn spawn_reconcile_queue_gauge(coalescer: Weak<DocumentSyncReconcileCoalescer>) {
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        return;
    };
    runtime.spawn(async move {
        let mut was_active = false;
        loop {
            sleep(QUEUE_LAG_INTERVAL).await;
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

async fn reconcile_inbound_document_sync_topics(
    context: &Arc<DriverContext>,
    topics: Vec<irokle::TopicId>,
) {
    let Some(net_handle) = context.net_handle.clone() else {
        return;
    };
    let run_started = Instant::now();
    let topic_count = topics.len();
    let targets = match net_handle.reconcile_document_sync_topics(topics).await {
        Ok(targets) => targets,
        Err(err) => {
            error!(error = ?err, "Failed to reconcile inbound document sync topics");
            return;
        }
    };
    let reconcile_elapsed = run_started.elapsed();
    let applied = targets.applied();
    debug!(applied, "Reconciled inbound document sync events");
    if applied == 0 {
        return;
    }
    let metadata_graph_tombstones = targets.metadata_graph_tombstones.clone();
    let realm_config_changed = targets
        .targets
        .iter()
        .any(|target| matches!(target, DocumentSyncTarget::RealmConfig { .. }));
    if realm_config_changed {
        let operation = ProcessPlacementsOperation::new(PlacementConfig {
            realm_id: *net_handle.realm_id(),
            local_node_id: net_handle.node_id(),
        });
        if let Err(error) = drive(operation, context.as_ref()).await {
            error!(error = ?error, "Failed to process pending placements after document sync reconciliation");
        }
    }
    let project_started = Instant::now();
    project_inbound_metadata_create_events(context, targets).await;
    let project_elapsed = project_started.elapsed();
    let prune_started = Instant::now();
    process_metadata_graph_tombstones(context, metadata_graph_tombstones).await;
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
}

pub fn initialize_net_incoming(context: Arc<DriverContext>) {
    let Some(net_handle) = context.net_handle.clone() else {
        warn!("Cannot initialize inbound handling without net handle");
        return;
    };
    let metadata_handle = context.metadata_handle.clone();

    net_handle.set_inbound_handler(Arc::new(OperationsInboundHandler::new(context)));
    if let Some(metadata_handle) = metadata_handle {
        schedule_periodic_metadata_document_sync_maintenance(metadata_handle);
    }
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
            match alpn {
                Alpn::Bao => {
                    if let Some(mut blob_handle) = self.context.blob_handle.clone() {
                        let stream_id = blob_handle.store_connection(stream).await;
                        let Some(net_handle) = self.context.net_handle.as_ref() else {
                            error!(peer = %node_id, "Cannot handle incoming bao stream without net handle");
                            return;
                        };
                        let first_event = blob_handle
                            .send_blob_effect(BlobEffect::ReadMessage { stream_id })
                            .await;

                        match first_event {
                            Event::Blob(BlobEvent::MessageReceived { payload, .. }) => {
                                match VersionReplicationMessage::from_bytes(&payload) {
                                    Ok(VersionReplicationMessage::VersionManifest(manifest)) => {
                                        debug!(
                                            peer = %node_id,
                                            stream_id = %stream_id,
                                            bucket = %manifest.bucket,
                                            key = %manifest.key,
                                            version_id = %manifest.version_id,
                                            kind = ?manifest.kind,
                                            "Received inbound version replication manifest"
                                        );
                                        let op = IncomingVersionReplicationOperation::new(
                                            stream_id,
                                            net_handle.node_id(),
                                            *net_handle.realm_id(),
                                            manifest,
                                        );
                                        if let Err(err) = drive(op, self.context.as_ref()).await {
                                            error!(error = ?err, "Failed to process inbound version replication stream");
                                        }
                                    }
                                    _ => {
                                        error!(
                                            peer = %node_id,
                                            stream_id = %stream_id,
                                            "Unsupported inbound bao payload"
                                        );
                                        let close_event = blob_handle
                                            .send_blob_effect(BlobEffect::CloseConnection { stream_id })
                                            .await;
                                        if let Event::Blob(BlobEvent::Error(err)) = close_event {
                                            error!(error = ?err, "Failed to close unsupported inbound bao stream");
                                        }
                                    }
                                }
                            }
                            Event::Blob(BlobEvent::Error(err)) => {
                                error!(error = ?err, "Failed to read initial inbound bao payload");
                            }
                            other => {
                                error!(event = ?other, "Unexpected first event for inbound bao stream");
                            }
                        }
                    } else {
                        error!("Cannot handle incoming bao stream without blob handle");
                    }
                }
                Alpn::DocumentSync => {
                    let Some(net_handle) = self.context.net_handle.clone() else {
                        warn!(node_id = %node_id, "Dropping inbound document sync stream without net handle");
                        return;
                    };
                    match net_handle.handle_document_sync_stream(stream, node_id).await {
                        Ok(touched_topics) => {
                            self.document_sync_reconcile
                                .trigger(self.context.clone(), touched_topics);
                        }
                        Err(err) => error!(error = ?err, "Failed to process inbound document sync stream"),
                    }
                }
                Alpn::Metadata => {
                    let Some(metadata_handle) = self.context.metadata_handle.clone() else {
                        warn!(node_id = %node_id, "Dropping inbound metadata stream without metadata handle");
                        return;
                    };
                    if let Err(err) = metadata_handle.handle_inbound_stream(stream, node_id).await {
                        error!(error = ?err, "Failed to process inbound metadata stream");
                    }
                }
                Alpn::Dht => {
                    warn!(
                        node_id = %node_id,
                        "Ignoring inbound stream for non-stream ALPN"
                    );
                }
            }
        }
        .instrument(span)
        .await;
    }
}

async fn project_inbound_metadata_create_events(
    context: &DriverContext,
    reconciled: DocumentSyncReconcileResult,
) {
    if !reconciled.metadata_create_events.is_empty() {
        let local_node_id = context.net_handle.as_ref().map(|net| net.node_id());
        if let Err(error) = project_metadata_create_events(
            context,
            reconciled.metadata_create_events,
            local_node_id,
        )
        .await
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
        let DocumentSyncTarget::MetadataCreateEvent {
            document_id,
            event_id,
            ..
        } = target
        else {
            continue;
        };
        targets.push((document_id, event_id));
    }
    if let Err(error) = project_metadata_create_events_from_log(context, targets).await {
        error!(
            error = ?error,
            "Failed to project metadata create event batch from log after inbound document sync reconciliation"
        );
        schedule_projection_retry(context).await;
    }
}

async fn schedule_projection_retry(context: &DriverContext) {
    if let Err(error) =
        schedule_pending_metadata_projection_drain(context, METADATA_PROJECTION_RETRY_AFTER).await
    {
        warn!(error = ?error, "Failed to schedule metadata projection retry");
    }
}

fn schedule_periodic_metadata_document_sync_maintenance(metadata_handle: MetadataHandle) {
    let jitter = Duration::from_secs(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|now| now.subsec_nanos() as u64 % METADATA_DOCUMENT_SYNC_MAINTENANCE_JITTER_SECS)
            .unwrap_or(0),
    );
    tokio::spawn(async move {
        let mut cycle = 0usize;
        loop {
            sleep(METADATA_DOCUMENT_SYNC_MAINTENANCE_INTERVAL + jitter).await;
            cycle = cycle.saturating_add(1);
            run_metadata_document_sync_maintenance(&metadata_handle, "periodic", cycle).await;
        }
    });
}

async fn run_metadata_document_sync_maintenance(
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
    use aruna_core::events::StorageEvent;
    use aruna_core::keyspaces::TASK_TIMER_KEYSPACE;
    use aruna_core::task::{PersistedTaskTimer, TaskKey};
    use aruna_storage::FjallStorage;
    use aruna_tasks::TaskHandle;
    use tempfile::tempdir;

    #[tokio::test]
    async fn inbound_projection_failure_schedules_durable_projection_retry() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(TaskHandle::new()),
        };
        let document_id = ulid::Ulid::new();
        let event_id = ulid::Ulid::new();

        project_inbound_metadata_create_events(
            &context,
            DocumentSyncReconcileResult {
                targets: vec![DocumentSyncTarget::MetadataCreateEvent {
                    document_id,
                    event_id,
                }],
                metadata_create_events: Vec::new(),
                metadata_graph_tombstones: Vec::new(),
            },
        )
        .await;

        let timer = read_persisted_task_timer(&storage, &TaskKey::DrainMetadataProjectionQueue)
            .await
            .expect("projection retry timer persisted");
        assert_eq!(timer.key, TaskKey::DrainMetadataProjectionQueue);
    }

    async fn read_persisted_task_timer(
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
}
