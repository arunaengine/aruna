use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use crate::driver::{DriverContext, drive};
use crate::metadata::MetadataHandle;
use crate::metadata::projector::{
    project_metadata_create_events, project_metadata_create_events_from_log,
};
use crate::process_placements::{PlacementConfig, ProcessPlacementsOperation};
use crate::replication::incoming_version_replication::IncomingVersionReplicationOperation;
use crate::replication::protocol::VersionReplicationMessage;
use aruna_core::alpn::Alpn;
use aruna_core::document::{DocumentSyncReconcileResult, DocumentSyncTarget};
use aruna_core::effects::BlobEffect;
use aruna_core::events::{BlobEvent, Event};
use aruna_core::handle::Handle;
use aruna_core::id::NodeId;
use aruna_core::telemetry::{QUEUE_LAG_INTERVAL, duration_ms};
use aruna_net::InboundEventHandler;
use aruna_net::streams::BiStream;
use async_trait::async_trait;
use tokio::time::sleep;
use tracing::{Instrument, debug, error, info, info_span, trace, warn};

const METADATA_IROKLE_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(60);
const METADATA_IROKLE_MAINTENANCE_JITTER_SECS: u64 = 15;
const METADATA_PROJECTION_RETRY_AFTER: Duration = Duration::from_secs(5);

#[derive(Debug)]
struct OperationsInboundHandler {
    context: Arc<DriverContext>,
    irokle_reconcile: Arc<IrokleReconcileCoalescer>,
}

impl OperationsInboundHandler {
    fn new(context: Arc<DriverContext>) -> Self {
        let irokle_reconcile = Arc::new(IrokleReconcileCoalescer::default());
        spawn_reconcile_queue_gauge(Arc::downgrade(&irokle_reconcile));
        Self {
            context,
            irokle_reconcile,
        }
    }
}

// Coalesces concurrent inbound reconcile triggers: one run in flight, all
// further triggers fold their topic sets into a single queued re-run.
#[derive(Debug, Default)]
struct IrokleReconcileCoalescer {
    state: Mutex<IrokleReconcileQueue>,
}

#[derive(Debug, Default)]
struct IrokleReconcileQueue {
    running: bool,
    queued: BTreeSet<irokle::TopicId>,
    queued_since: Option<Instant>,
}

impl IrokleReconcileCoalescer {
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
                reconcile_inbound_irokle_topics(&context, batch).await;
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
fn spawn_reconcile_queue_gauge(coalescer: Weak<IrokleReconcileCoalescer>) {
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

async fn reconcile_inbound_irokle_topics(
    context: &Arc<DriverContext>,
    topics: Vec<irokle::TopicId>,
) {
    let Some(net_handle) = context.net_handle.clone() else {
        return;
    };
    let run_started = Instant::now();
    let topic_count = topics.len();
    let targets = match net_handle.reconcile_irokle_topics(topics).await {
        Ok(targets) => targets,
        Err(err) => {
            error!(error = ?err, "Failed to reconcile inbound irokle topics");
            return;
        }
    };
    let reconcile_elapsed = run_started.elapsed();
    let applied = targets.applied();
    debug!(applied, "Reconciled inbound Irokle document events");
    if applied == 0 {
        return;
    }
    let lifecycle_graphs = targets
        .targets
        .iter()
        .filter_map(|target| match target {
            DocumentSyncTarget::MetadataGraphLifecycle { graph_iri } => Some(graph_iri.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
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
            error!(error = ?error, "Failed to process pending topic placements after Irokle reconciliation");
        }
    }
    let project_started = Instant::now();
    project_inbound_metadata_create_events(context, targets).await;
    let project_elapsed = project_started.elapsed();
    let prune_started = Instant::now();
    prune_inbound_deleted_graphs(context, lifecycle_graphs).await;
    info!(
        event = "pipeline.reconcile.summary",
        topics = topic_count,
        applied,
        reconcile_ms = duration_ms(reconcile_elapsed),
        project_ms = duration_ms(project_elapsed),
        prune_ms = duration_ms(prune_started.elapsed()),
        total_ms = duration_ms(run_started.elapsed()),
        "Inbound Irokle reconcile summary"
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
        schedule_periodic_metadata_irokle_maintenance(metadata_handle);
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
                                            "Unsupported inbound bao payload; legacy raw bao replication is no longer supported"
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
                Alpn::Irokle => {
                    let Some(net_handle) = self.context.net_handle.clone() else {
                        warn!(node_id = %node_id, "Dropping inbound irokle stream without net handle");
                        return;
                    };
                    match net_handle.handle_irokle_stream(stream, node_id).await {
                        Ok(touched_topics) => {
                            self.irokle_reconcile
                                .trigger(self.context.clone(), touched_topics);
                        }
                        Err(err) => error!(error = ?err, "Failed to process inbound irokle stream"),
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
                "Failed to project metadata create event batch after inbound Irokle reconciliation"
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
            "Failed to project metadata create event batch from log after inbound Irokle reconciliation"
        );
        schedule_projection_retry(context).await;
    }
}

async fn schedule_projection_retry(context: &DriverContext) {
    let Some(task_handle) = context.task_handle.as_ref() else {
        return;
    };
    let event = task_handle
        .send_effect(aruna_core::effects::Effect::Task(
            aruna_core::task::TaskEffect::ResetTimer {
                key: aruna_core::task::TaskKey::DrainMetadataProjectionQueue,
                after: METADATA_PROJECTION_RETRY_AFTER,
            },
        ))
        .await;
    if let Event::Task(aruna_core::task::TaskEvent::Error { message, .. }) = event {
        warn!(message = %message, "Failed to schedule metadata projection retry");
    }
}

fn schedule_periodic_metadata_irokle_maintenance(metadata_handle: MetadataHandle) {
    let jitter = Duration::from_secs(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|now| now.subsec_nanos() as u64 % METADATA_IROKLE_MAINTENANCE_JITTER_SECS)
            .unwrap_or(0),
    );
    tokio::spawn(async move {
        let mut cycle = 0usize;
        loop {
            sleep(METADATA_IROKLE_MAINTENANCE_INTERVAL + jitter).await;
            cycle = cycle.saturating_add(1);
            run_metadata_irokle_maintenance(&metadata_handle, "periodic", cycle).await;
        }
    });
}

async fn prune_inbound_deleted_graphs(context: &DriverContext, graphs: Vec<String>) {
    if graphs.is_empty() {
        return;
    }
    let Some(metadata_handle) = context.metadata_handle.clone() else {
        return;
    };
    for graph_iri in graphs {
        if let Err(error) = metadata_handle
            .prune_graph_if_deleted(graph_iri.clone())
            .await
        {
            warn!(graph_iri = %graph_iri, error = ?error, "Failed to prune deleted metadata graph");
        }
    }
}

async fn run_metadata_irokle_maintenance(
    metadata_handle: &MetadataHandle,
    source: &'static str,
    attempt: usize,
) {
    if let Err(error) = metadata_handle.reconcile_irokle().await {
        warn!(
            source,
            attempt,
            error = ?error,
            "Craqle Irokle reconciliation failed"
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
