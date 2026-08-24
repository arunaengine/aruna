use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use crate::blob::resolve_blob_permission_paths::ResolveBlobPermissionPathsOperation;
use crate::dashboard::{notify_dashboard_change, targets_change_dashboard};
use crate::document_sync_outbox::{
    new_outbox_record_with_id, schedule_outbox_drain_effect, write_outbox_effect,
};
use crate::driver::{
    DriverContext, drive, gate_context, node_routing, now_ms, quota_marked_routing,
};
use crate::get_realm_config::GetRealmConfigOperation;
use crate::jobs::runtime::JobsRuntime;
use crate::metadata::MetadataHandle;
use crate::metadata::projector::{
    METADATA_PROJECTION_RETRY_AFTER, project_metadata_create_events,
    project_metadata_create_events_from_log, schedule_pending_metadata_projection_drain,
};
use crate::metadata::prune_queue::process_metadata_graph_tombstones;
use crate::notifications::watch::emit::emit_resource_watch_event;
use crate::notifications::watch::interest::refresh_watch_interest_for_targets;
use crate::permission_rules::GroupPermissionRules;
use crate::process_placements::reconcile_shard_topics;
use crate::queue_backoff::queue_retry_after_ms;
use crate::replication::bao_read::IncomingBaoReadOperation;
use crate::replication::incoming_version_replication::{
    IncomingVersionReplicationOperation, IncomingVersionReplicationResult,
};
use crate::replication::location_summary::LocationSummaryOperation;
use crate::replication::protocol::{VersionReplicationManifest, VersionReplicationMessage};
use crate::request_authorization::{AuthorizeError, authorize};
use crate::request_policy::{
    PolicyEnforcementError, PolicyEvaluator, PolicyRequestExtras, policy_request_with,
};
use crate::s3::get_bucket_info::{GetBucketInfoError, GetBucketInfoOperation};
use crate::usage_stats::refresh_realm_usage_summary_for_targets;
use aruna_core::alpn::Alpn;
use aruna_core::document::{
    DocumentSyncEvictedDocument, DocumentSyncReconcileResult, DocumentSyncTarget,
};
use aruna_core::effects::BlobEffect;
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::id::NodeId;
use aruna_core::shutdown::Shutdown;
use aruna_core::structs::{
    AuthContext, HashPathIndexKey, Permission, RealmId, ReplicationItemKind, RoCrateLimits,
    WatchEvent, WatchEventDetail, WatchEventKind, blob_bucket_permission_path,
    blob_object_permission_path, data_watch_resource_path,
};
use aruna_core::task::{TaskEvent, TaskKey};
use aruna_core::telemetry::{QUEUE_LAG_INTERVAL, duration_ms};
use aruna_net::InboundEventHandler;
use aruna_net::streams::BiStream;
use async_trait::async_trait;
use tokio::time::{sleep, timeout};
use tracing::{Instrument, debug, error, info, info_span, trace, warn};
use ulid::Ulid;

const METADATA_DOCUMENT_SYNC_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(60);
const METADATA_DOCUMENT_SYNC_MAINTENANCE_JITTER_SECS: u64 = 15;
const INBOUND_BAO_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug)]
struct OperationsInboundHandler {
    context: Arc<DriverContext>,
    document_sync_reconcile: Arc<DocumentSyncReconcileCoalescer>,
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
        let document_sync_reconcile =
            Arc::new(DocumentSyncReconcileCoalescer::new(shutdown.clone()));
        spawn_queue_gauge(Arc::downgrade(&document_sync_reconcile), &shutdown);
        Self {
            context,
            document_sync_reconcile,
            rocrate_limits,
            jobs_runtime,
        }
    }

    /// Blob replication is trusted only from realm nodes eligible to hold and
    /// sync data; unknown or user-kind peers are rejected. A device serves the
    /// same set: the realm nodes pull its offered content, and device-to-device
    /// transfer is not a path. Fails closed when the config is unreadable.
    async fn bao_peer_admitted(&self, realm_id: RealmId, peer: NodeId) -> bool {
        let config = match drive(
            GetRealmConfigOperation::new(realm_id),
            self.context.as_ref(),
        )
        .await
        {
            Ok(config) => config,
            Err(error) => {
                warn!(peer = %peer, error = %error, "Failed to read realm config for replication gate");
                return false;
            }
        };
        match config.sync_eligible_node_ids() {
            Ok(ids) => ids.contains(&peer),
            Err(error) => {
                warn!(peer = %peer, error = %error, "Failed to resolve replication peers");
                false
            }
        }
    }
}

async fn emit_replication_watch(
    context: &DriverContext,
    node_id: aruna_core::NodeId,
    manifest: &VersionReplicationManifest,
    result: &IncomingVersionReplicationResult,
) {
    let Some(group_id) = result.group_id else {
        return;
    };
    if !result.applied || manifest.kind != ReplicationItemKind::Materialized {
        return;
    }
    let size_bytes = manifest.blob.as_ref().map_or(0, |blob| blob.size);
    let occurred_at_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    emit_resource_watch_event(
        context,
        WatchEvent {
            event_id: Ulid::generate(),
            realm_id: manifest.auth_context.realm_id,
            kind: WatchEventKind::DataUploaded,
            path: data_watch_resource_path(group_id, node_id, &manifest.bucket, &manifest.key),
            actor: manifest.auth_context.user_id,
            occurred_at_ms,
            detail: WatchEventDetail::DataUploaded {
                group_id,
                node_id,
                bucket: manifest.bucket.clone(),
                key: manifest.key.clone(),
                size_bytes,
            },
        },
    )
    .await;
}

async fn allow_policy(
    context: &DriverContext,
    auth: &AuthContext,
    path: &str,
    permission: &Permission,
    operation: &str,
) -> Result<bool, String> {
    match authorize(
        context,
        auth.realm_id,
        auth,
        path,
        permission,
        PolicyRequestExtras::operation(operation),
    )
    .await
    {
        Ok(()) => Ok(true),
        Err(AuthorizeError::PermissionDenied)
        | Err(AuthorizeError::Policy(PolicyEnforcementError::Denied { .. })) => Ok(false),
        Err(error) => Err(error.to_string()),
    }
}

fn auth_matches(auth: &AuthContext, realm_id: RealmId) -> bool {
    auth.realm_id == realm_id && auth.user_id.realm_id == realm_id
}

async fn manifest_policy(
    context: &DriverContext,
    local_realm: RealmId,
    local_node: NodeId,
    manifest: &VersionReplicationManifest,
) -> Result<(Option<String>, Option<String>), String> {
    let group_id = match drive(
        GetBucketInfoOperation::new(manifest.bucket.clone()),
        context,
    )
    .await
    {
        Ok(Some(Ok(info))) => info.group_id,
        Ok(None) | Ok(Some(Err(GetBucketInfoError::NotFound))) => manifest.group_id,
        Ok(Some(Err(error))) => return Err(error.to_string()),
        Err(error) => return Err(error.to_string()),
    };
    let path = if manifest.key.is_empty() {
        blob_bucket_permission_path(local_realm, group_id, local_node, &manifest.bucket)
    } else {
        blob_object_permission_path(
            local_realm,
            group_id,
            local_node,
            &manifest.bucket,
            &manifest.key,
        )
    };
    if manifest.reference_advance.is_some() {
        // The sync-eligible publisher attests source READ for this advance.
        return Ok(if auth_matches(&manifest.auth_context, local_realm) {
            (Some(path), None)
        } else {
            (None, None)
        });
    }
    let operation = if manifest.kind == ReplicationItemKind::DeleteMarker {
        "s3.DeleteObject"
    } else {
        "s3.PutObject"
    };
    if !auth_matches(&manifest.auth_context, local_realm)
        || !allow_policy(
            context,
            &manifest.auth_context,
            &path,
            &Permission::WRITE,
            operation,
        )
        .await?
    {
        return Ok((None, None));
    }
    let manifest_path = Some(path.clone());
    let writer_path = if let Some(auth) = manifest.writer_auth_context.as_ref()
        && auth_matches(auth, local_realm)
    {
        if allow_policy(context, auth, &path, &Permission::WRITE, operation).await? {
            Some(path)
        } else {
            None
        }
    } else {
        None
    };
    Ok((manifest_path, writer_path))
}

async fn bao_policy(
    context: &DriverContext,
    local_realm: RealmId,
    local_node: NodeId,
    request: &crate::replication::protocol::BaoReadRequest,
) -> Result<(HashSet<String>, Vec<HashPathIndexKey>, bool), String> {
    let mut paths = HashSet::new();
    if request.realm_id != local_realm || !auth_matches(&request.auth_context, request.realm_id) {
        return Ok((paths, Vec::new(), false));
    }
    match &request.target {
        crate::replication::protocol::BaoReadTarget::ExactVersion(target) => {
            if target.realm_id != request.realm_id || target.node_id != local_node {
                return Ok((paths, Vec::new(), false));
            }
            let group_id =
                match drive(GetBucketInfoOperation::new(target.bucket.clone()), context).await {
                    Ok(Some(Ok(info))) => info.group_id,
                    Ok(None) | Ok(Some(Err(GetBucketInfoError::NotFound))) => {
                        return Ok((paths, Vec::new(), false));
                    }
                    Ok(Some(Err(error))) => return Err(error.to_string()),
                    Err(error) => return Err(error.to_string()),
                };
            let path = blob_object_permission_path(
                request.realm_id,
                group_id,
                local_node,
                &target.bucket,
                &target.key,
            );
            if allow_policy(
                context,
                &request.auth_context,
                &path,
                &Permission::READ,
                "s3.GetObject",
            )
            .await?
            {
                paths.insert(path);
            }
            Ok((paths, Vec::new(), false))
        }
        crate::replication::protocol::BaoReadTarget::Blake3(hash) => {
            let candidates = drive(ResolveBlobPermissionPathsOperation::new(*hash), context)
                .await
                .map_err(|error| error.to_string())?;
            let mut unique = BTreeMap::new();
            for candidate in candidates {
                if candidate.realm_id != request.realm_id
                    || candidate.node_id != local_node
                    || candidate.blake3_hash != *hash
                {
                    continue;
                }
                let key = candidate.to_bytes().map_err(|error| error.to_string())?;
                unique.entry(key).or_insert(candidate);
            }
            let candidates = unique.into_values().collect::<Vec<_>>();
            let evaluators = PolicyEvaluator::load_bulk(
                context,
                candidates
                    .iter()
                    .map(|candidate| (candidate.realm_id, candidate.group_id)),
            )
            .await
            .map_err(|error| error.to_string())?;
            let permissions = GroupPermissionRules::collect(
                context,
                Some(&request.auth_context),
                candidates.iter().map(|candidate| candidate.group_id),
            )
            .await;
            let mut allowed = Vec::with_capacity(candidates.len());
            let mut had_denial = false;
            for candidate in candidates {
                let path = candidate.permission_path();
                if !permissions.allows(candidate.group_id, &path, &Permission::READ) {
                    had_denial = true;
                    continue;
                }
                let request = policy_request_with(
                    &path,
                    &Permission::READ,
                    Some(&request.auth_context.user_id),
                    PolicyRequestExtras::operation("s3.GetObject"),
                );
                let evaluator = evaluators
                    .get(&(candidate.realm_id, candidate.group_id))
                    .ok_or_else(|| "hash policy evaluator is unavailable".to_string())?;
                match evaluator.evaluate(&request) {
                    Ok(()) => {
                        paths.insert(path);
                        allowed.push(candidate);
                    }
                    Err(PolicyEnforcementError::Denied { .. }) => had_denial = true,
                    Err(error) => return Err(error.to_string()),
                }
            }
            Ok((paths, allowed, had_denial))
        }
    }
}

// Coalesces concurrent inbound reconcile triggers: one run in flight, all
// further triggers fold their topic sets into a single queued re-run.
#[derive(Debug, Default)]
struct DocumentSyncReconcileCoalescer {
    state: Mutex<DocumentSyncReconcileQueue>,
    shutdown: Shutdown,
}

#[derive(Debug, Default)]
struct DocumentSyncReconcileQueue {
    running: bool,
    queued: BTreeSet<irokle::TopicId>,
    queued_since: Option<Instant>,
}

impl DocumentSyncReconcileCoalescer {
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
                if reconcile_inbound_document_sync_topics(&context, batch.clone()).await {
                    failures = 0;
                } else {
                    coalescer.trigger(context.clone(), batch);
                    let retry_after = Duration::from_millis(queue_retry_after_ms(failures));
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
fn spawn_queue_gauge(coalescer: Weak<DocumentSyncReconcileCoalescer>, shutdown: &Shutdown) {
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

async fn reconcile_inbound_document_sync_topics(
    context: &Arc<DriverContext>,
    topics: Vec<irokle::TopicId>,
) -> bool {
    let Some(net_handle) = context.net_handle.clone() else {
        return true;
    };
    let run_started = Instant::now();
    let topic_count = topics.len();
    let targets = match net_handle.reconcile_document_sync_topics(topics).await {
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
        .any(|target| matches!(target, DocumentSyncTarget::RealmConfig { .. }));
    if realm_config_changed {
        // Transition-free: a heavy transition step here would stall document
        // application; the reconcile arms the SyncPlacements timer instead.
        reconcile_shard_topics(context, *net_handle.realm_id(), net_handle.node_id()).await;
    }
    refresh_realm_usage_summary_for_targets(context, net_handle.node_id(), &targets.targets).await;
    refresh_watch_interest_for_targets(context, &targets.targets).await;
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
    if dashboard_changed {
        notify_dashboard_change(context);
    }
    true
}

pub fn initialize_net_incoming(context: Arc<DriverContext>) {
    initialize_net_holder(
        context,
        RoCrateLimits::default(),
        JobsRuntime::new(),
        &Shutdown::new(),
    );
}

pub fn initialize_net_holder(
    context: Arc<DriverContext>,
    rocrate_limits: RoCrateLimits,
    jobs_runtime: Arc<JobsRuntime>,
    shutdown: &Shutdown,
) {
    let Some(net_handle) = context.net_handle.clone() else {
        warn!("Cannot initialize inbound handling without net handle");
        return;
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
        schedule_periodic_metadata_document_sync_maintenance(
            context,
            Arc::downgrade(&inbound_handler.document_sync_reconcile),
            metadata_handle,
            shutdown,
        );
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
            // Debug poll frames reserve stack for every branch at once, so each
            // protocol branch is boxed to keep only the active one on the stack.
            match alpn {
                Alpn::Bao => {
                    Box::pin(async {
                        if let Some(blob_handle) = self.context.blob_handle.clone() {
                            let Some(net_handle) = self.context.net_handle.clone() else {
                                error!(peer = %node_id, "Cannot handle incoming bao stream without net handle");
                                return;
                            };
                            // #332: only an authenticated sync-eligible realm peer
                            // may open the blob replication plane at all.
                            let eligible = timeout(
                                INBOUND_BAO_TIMEOUT,
                                self.bao_peer_admitted(*net_handle.realm_id(), node_id),
                            )
                            .await
                            .unwrap_or(false);
                            if !eligible {
                                warn!(peer = %node_id, "Rejecting bao stream from non-sync-eligible peer");
                                close_bao_stream(stream);
                                return;
                            }
                            let stream_id = match timeout(
                                INBOUND_BAO_TIMEOUT,
                                blob_handle.store_connection(node_id, stream),
                            )
                            .await
                            {
                                Ok(Ok(stream_id)) => stream_id,
                                Ok(Err(err)) => {
                                    error!(peer = %node_id, error = ?err, "Failed to register inbound bao stream");
                                    return;
                                }
                                Err(_) => {
                                    warn!(peer = %node_id, "Timed out registering inbound bao stream");
                                    return;
                                }
                            };
                            let eligible = timeout(
                                INBOUND_BAO_TIMEOUT,
                                self.bao_peer_admitted(*net_handle.realm_id(), node_id),
                            )
                            .await
                            .unwrap_or(false);
                            if !eligible {
                                warn!(peer = %node_id, "Rejecting bao stream from non-sync-eligible peer");
                                close_failed_bao(&blob_handle, stream_id).await;
                                return;
                            }
                            let first_event = blob_handle
                                .send_blob_effect(BlobEffect::ReadMessage { stream_id });
                            let first_event = match timeout(INBOUND_BAO_TIMEOUT, first_event).await {
                                Ok(event) => event,
                                Err(_) => {
                                    warn!(peer = %node_id, stream_id = %stream_id, "Timed out reading inbound bao control message");
                                    close_failed_bao(&blob_handle, stream_id).await;
                                    return;
                                }
                            };

                            match first_event {
                                Event::Blob(BlobEvent::MessageReceived { payload, .. }) => {
                                    match VersionReplicationMessage::from_bytes(&payload) {
                                        Ok(VersionReplicationMessage::VersionManifest(manifest))
                                        | Ok(VersionReplicationMessage::ReferenceAdvance {
                                            manifest,
                                            ..
                                        }) => {
                                            debug!(
                                                peer = %node_id,
                                                stream_id = %stream_id,
                                                bucket = %manifest.bucket,
                                                key = %manifest.key,
                                                version_id = %manifest.version_id,
                                                kind = ?manifest.kind,
                                                "Received inbound version replication manifest"
                                            );
                                            let watch_manifest = manifest.clone();
                                            // Only a materialized item can place a
                                            // blob, so only it reads the caps.
                                            let routing = if manifest.kind
                                                == ReplicationItemKind::Materialized
                                            {
                                                match quota_marked_routing(self.context.as_ref()).await
                                                {
                                                    Ok(routing) => routing,
                                                    Err(error) => {
                                                        error!(peer = %node_id, error = %error, "Refusing inbound replication with unreadable routing inputs");
                                                        close_failed_bao(&blob_handle, stream_id).await;
                                                        return;
                                                    }
                                                }
                                            } else {
                                                node_routing(self.context.as_ref())
                                            };
                                            let (manifest_path, writer_path) = match manifest_policy(
                                                self.context.as_ref(),
                                                *net_handle.realm_id(),
                                                net_handle.node_id(),
                                                &manifest,
                                            )
                                            .await
                                            {
                                                Ok(path) => path,
                                                Err(error) => {
                                                    error!(peer = %node_id, stream_id = %stream_id, error = %error, "Refusing inbound replication with unavailable request policy");
                                                    close_failed_bao(&blob_handle, stream_id).await;
                                                    return;
                                                }
                                            };
                                            let gate = match gate_context(
                                                self.context.as_ref(),
                                                *net_handle.realm_id(),
                                                now_ms(),
                                            )
                                            .await
                                            {
                                                Ok(gate) => gate,
                                                Err(error) => {
                                                    error!(peer = %node_id, error = %error, "Refusing inbound replication with unreadable placement subject");
                                                    close_failed_bao(&blob_handle, stream_id).await;
                                                    return;
                                                }
                                            };
                                            let mut op = IncomingVersionReplicationOperation::new(
                                                stream_id,
                                                net_handle.node_id(),
                                                *net_handle.realm_id(),
                                                manifest,
                                            )
                                            .with_routing(routing)
                                            .with_rocrate_limits(self.rocrate_limits.clone())
                                            .with_publisher_node(node_id)
                                            .with_manifest_policy(manifest_path)
                                            .with_writer_policy(writer_path);
                                            if let Some(gate) = gate {
                                                op = op.with_gate(gate);
                                            }
                                            match drive(op, self.context.as_ref()).await {
                                                Ok(Ok(result)) => {
                                                    emit_replication_watch(
                                                        self.context.as_ref(),
                                                        net_handle.node_id(),
                                                        &watch_manifest,
                                                        &result,
                                                    )
                                                    .await;
                                                }
                                                Ok(Err(err)) | Err(err) => {
                                                    error!(error = ?err, "Failed to process inbound version replication stream");
                                                    close_failed_bao(&blob_handle, stream_id).await;
                                                }
                                            }
                                        }
                                        Ok(VersionReplicationMessage::BaoReadRequest(request)) => {
                                            let (policy_paths, policy_candidates, had_denial) =
                                                match bao_policy(
                                                self.context.as_ref(),
                                                *net_handle.realm_id(),
                                                net_handle.node_id(),
                                                &request,
                                            )
                                            .await
                                            {
                                                Ok(result) => result,
                                                Err(error) => {
                                                    error!(peer = %node_id, stream_id = %stream_id, error = %error, "Refusing inbound bao read with unavailable request policy");
                                                    close_failed_bao(&blob_handle, stream_id).await;
                                                    return;
                                                }
                                            };
                                            let op = IncomingBaoReadOperation::new(
                                                node_id,
                                                net_handle.node_id(),
                                                *net_handle.realm_id(),
                                                stream_id,
                                                request,
                                            )
                                            .with_policy_paths(policy_paths)
                                            .with_policy_candidates(policy_candidates, had_denial)
                                            .with_now(now_ms())
                                            .with_snapshot();
                                            if let Err(error) =
                                                drive(op, self.context.as_ref()).await
                                            {
                                                error!(
                                                    peer = %node_id,
                                                    stream_id = %stream_id,
                                                    error = ?error,
                                                    "Failed to process inbound bao read"
                                                );
                                                close_failed_bao(&blob_handle, stream_id).await;
                                            }
                                        }
                                        Ok(VersionReplicationMessage::LocationSummaryRequest(
                                            request,
                                        )) => {
                                            let identity_allowed = request.realm_id
                                                == *net_handle.realm_id()
                                                && auth_matches(
                                                    &request.auth_context,
                                                    *net_handle.realm_id(),
                                                );
                                            let op = LocationSummaryOperation::new_incoming(
                                                node_id,
                                                net_handle.node_id(),
                                                stream_id,
                                                request,
                                            )
                                            .with_policy(identity_allowed);
                                            if let Err(error) = drive(op, self.context.as_ref()).await {
                                                error!(
                                                    peer = %node_id,
                                                    stream_id = %stream_id,
                                                    error = ?error,
                                                    "Failed to answer inbound location summary"
                                                );
                                                close_failed_bao(&blob_handle, stream_id).await;
                                            }
                                        }
                                        _ => {
                                            error!(
                                                peer = %node_id,
                                                stream_id = %stream_id,
                                                "Unsupported inbound bao payload"
                                            );
                                            close_failed_bao(&blob_handle, stream_id).await;
                                        }
                                    }
                                }
                                Event::Blob(BlobEvent::Error(err)) => {
                                    error!(error = ?err, "Failed to read initial inbound bao payload");
                                    close_failed_bao(&blob_handle, stream_id).await;
                                }
                                other => {
                                    error!(event = ?other, "Unexpected first event for inbound bao stream");
                                    close_failed_bao(&blob_handle, stream_id).await;
                                }
                            }
                        } else {
                            error!("Cannot handle incoming bao stream without blob handle");
                        }
                    })
                    .await
                }
                Alpn::DocumentSync => {
                    Box::pin(async {
                        let Some(net_handle) = self.context.net_handle.clone() else {
                            warn!(peer = %node_id, "Dropping inbound document sync stream without net handle");
                            return;
                        };
                        match net_handle.handle_document_sync_stream(stream, node_id).await {
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
                    })
                    .await
                }
                Alpn::Metadata => {
                    Box::pin(async {
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
                    })
                    .await
                }
                Alpn::NativeReference => {
                    Box::pin(crate::native_reference::handle_native_stream(
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

    async fn handle_evicted_documents(&self, documents: Vec<DocumentSyncEvictedDocument>) -> bool {
        reemit_evicted_documents(self.context.as_ref(), documents).await
    }
}

async fn close_failed_bao(blob_handle: &aruna_blob::blob::BlobHandle, stream_id: ulid::Ulid) {
    let close_event = blob_handle
        .send_blob_effect(BlobEffect::CloseConnection { stream_id })
        .await;
    if let Event::Blob(BlobEvent::Error(err)) = close_event {
        error!(error = ?err, "Failed to close rejected inbound bao stream");
    }
}

fn close_bao_stream(mut stream: BiStream) {
    _ = stream.0.finish();
    _ = stream.1.stop(0u32.into());
}

/// Re-enqueues the payloads recovered from a genesis tie-break eviction as
/// document-sync outbox records so they replay onto the winning chain through
/// the normal drain. Every record reuses the evicted event's own id, which is
/// the whole safety story: the outbox key is derived from it and appliers dedupe
/// on it, so repeating this conversion rewrites the same rows instead of
/// duplicating them. Every record uses `allow_genesis: false` (the loser must
/// not mint a rival genesis) and empty peers (resolved to the realm default set
/// at the net layer, exactly like the mutation operations that originate admin
/// events).
///
/// Returns whether every record is durable. Anything less keeps the caller's
/// journal entry so the payload is retried instead of lost.
async fn reemit_evicted_documents(
    context: &DriverContext,
    documents: Vec<DocumentSyncEvictedDocument>,
) -> bool {
    let Some(net_handle) = context.net_handle.as_ref() else {
        warn!(task_id = ?TaskKey::DrainDocumentSyncOutbox, "Cannot re-emit evicted documents without net handle");
        return false;
    };
    let node_id = net_handle.node_id();
    let mut written = 0usize;
    let mut complete = true;
    for document in documents {
        let record = new_outbox_record_with_id(
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
                warn!(task_id = ?TaskKey::DrainDocumentSyncOutbox, error = %error, "Failed to encode re-emitted eviction outbox record");
                complete = false;
                continue;
            }
        };
        match context.storage_handle.send_effect(effect).await {
            Event::Storage(StorageEvent::WriteResult { .. }) => written += 1,
            Event::Storage(StorageEvent::Error { error }) => {
                warn!(task_id = ?TaskKey::DrainDocumentSyncOutbox, error = %error, "Failed to write re-emitted eviction outbox record");
                complete = false;
            }
            other => {
                warn!(task_id = ?TaskKey::DrainDocumentSyncOutbox, event = ?other, "Unexpected event writing re-emitted eviction outbox record");
                complete = false;
            }
        }
    }
    if written == 0 {
        return complete;
    }
    let Some(task_handle) = context.task_handle.as_ref() else {
        warn!(task_id = ?TaskKey::DrainDocumentSyncOutbox, "Cannot schedule outbox drain for re-emitted evictions without task handle");
        return complete;
    };
    if let Event::Task(TaskEvent::Error { message, .. }) = task_handle
        .send_effect(schedule_outbox_drain_effect())
        .await
    {
        warn!(task_id = ?TaskKey::DrainDocumentSyncOutbox, message = %message, "Failed to schedule outbox drain after re-emitting evictions");
    }
    info!(
        count = written,
        "Re-emitted evicted documents to the sync outbox"
    );
    complete
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
        warn!(task_id = ?TaskKey::DrainMetadataProjectionQueue, error = ?error, "Failed to schedule metadata projection retry");
    }
}

fn schedule_periodic_metadata_document_sync_maintenance(
    context: Arc<DriverContext>,
    coalescer: Weak<DocumentSyncReconcileCoalescer>,
    metadata_handle: MetadataHandle,
    shutdown: &Shutdown,
) {
    let jitter = Duration::from_secs(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|now| now.subsec_nanos() as u64 % METADATA_DOCUMENT_SYNC_MAINTENANCE_JITTER_SECS)
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
                _ = sleep(METADATA_DOCUMENT_SYNC_MAINTENANCE_INTERVAL + jitter) => {}
            }
            let Some(coalescer) = coalescer.upgrade() else {
                return;
            };
            coalescer.trigger_all(context.clone());
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
    use crate::replication::protocol::ReferenceAdvance;
    use aruna_blob::blob::BlobHandler;
    use aruna_core::UserId;
    use aruna_core::events::StorageEvent;
    use aruna_core::keyspaces::TASK_TIMER_KEYSPACE;
    use aruna_core::structs::{
        Backend, BackendConfig, PathRestriction, PortableSourceDescriptor, SourceConnectorKind,
        SourceMetadata, StagingStrategy, VersionSourceBinding,
    };
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

    #[tokio::test]
    async fn rejects_foreign_peer() {
        // The blob replication plane must refuse peers that are unknown or not
        // sync-eligible before any manifest is read.
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let realm_id = aruna_core::structs::RealmId::from_bytes([3u8; 32]);
        let server = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let user = iroh::SecretKey::from_bytes(&[2u8; 32]).public();
        let mut config =
            aruna_core::structs::RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        config.ensure_node(server, aruna_core::structs::RealmNodeKind::Server);
        config.ensure_node(
            user,
            aruna_core::structs::RealmNodeKind::User {
                owner: aruna_core::UserId::nil(realm_id),
            },
        );
        let actor = aruna_core::structs::Actor {
            node_id: server,
            user_id: aruna_core::UserId::nil(realm_id),
            realm_id,
        };
        let target = DocumentSyncTarget::RealmConfig { realm_id };
        storage
            .send_storage_effect(aruna_core::effects::StorageEffect::Write {
                key_space: target.storage_keyspace().to_string(),
                key: target.storage_key(),
                value: config.to_bytes(&actor).unwrap().into(),
                txn_id: None,
            })
            .await;

        let handler = OperationsInboundHandler::new(
            Arc::new(DriverContext {
                storage_handle: storage,
                net_handle: None,
                blob_handle: None,
                metadata_handle: None,
                task_handle: None,
                compute_handle: None,
            }),
            RoCrateLimits::default(),
            JobsRuntime::new(),
            Shutdown::new(),
        );

        let unknown = iroh::SecretKey::from_bytes(&[9u8; 32]).public();
        assert!(handler.bao_peer_admitted(realm_id, server).await);
        // A device is never a replication source, and a device serving its own
        // content admits the realm's infrastructure only.
        assert!(!handler.bao_peer_admitted(realm_id, user).await);
        assert!(!handler.bao_peer_admitted(realm_id, unknown).await);
    }

    #[tokio::test]
    async fn checks_advance_policy() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let realm_id = RealmId::from_bytes([4u8; 32]);
        let foreign_realm = RealmId::from_bytes([5u8; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[6u8; 32]).public();
        let user_id = UserId::local(Ulid::generate(), realm_id);
        let group_id = Ulid::generate();
        let path = blob_object_permission_path(realm_id, group_id, node_id, "bucket", "key");
        let auth_context = AuthContext {
            user_id,
            realm_id,
            path_restrictions: Some(vec![PathRestriction {
                pattern: "/source/**".to_string(),
                permission: Permission::READ,
            }]),
        };
        let mut manifest = VersionReplicationManifest {
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            version_id: Ulid::generate(),
            group_id,
            kind: ReplicationItemKind::Materialized,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: user_id,
            current_version: true,
            current_version_generation: Some(2),
            auth_context,
            blob: None,
            source: Some(VersionSourceBinding {
                strategy: StagingStrategy::Reference,
                descriptor: PortableSourceDescriptor {
                    kind: SourceConnectorKind::Http,
                    public_config: HashMap::new(),
                    source_path: "source".to_string(),
                    version_selector: None,
                    capabilities: Vec::new(),
                    origin_node_id: None,
                },
                connector_id: Some(Ulid::generate()),
            }),
            multipart: None,
            reference_intent: true,
            origin: None,
            upstream_sources: Vec::new(),
            writer_auth_context: None,
            reference_metadata: Some(SourceMetadata {
                content_length: 1,
                content_type: None,
                etag: None,
                last_modified: None,
                source_version: None,
            }),
            metadata: HashMap::new(),
            reference_advance: Some(ReferenceAdvance {
                generation: 2,
                predecessor: Ulid::generate(),
            }),
            reference_advance_count: Some(0),
            placement_policies: Vec::new(),
        };

        assert_eq!(
            manifest_policy(&context, realm_id, node_id, &manifest)
                .await
                .unwrap(),
            (Some(path.clone()), None)
        );

        manifest.auth_context = AuthContext::anonymous(realm_id);
        assert_eq!(
            manifest_policy(&context, realm_id, node_id, &manifest)
                .await
                .unwrap(),
            (Some(path), None)
        );

        manifest.auth_context = AuthContext {
            user_id: UserId::nil(foreign_realm),
            realm_id,
            path_restrictions: None,
        };
        assert_eq!(
            manifest_policy(&context, realm_id, node_id, &manifest)
                .await
                .unwrap(),
            (None, None)
        );

        manifest.auth_context = AuthContext::anonymous(foreign_realm);
        assert_eq!(
            manifest_policy(&context, realm_id, node_id, &manifest)
                .await
                .unwrap(),
            (None, None)
        );

        manifest.reference_advance = None;
        manifest.auth_context = AuthContext::anonymous(realm_id);
        manifest.writer_auth_context = Some(manifest.auth_context.clone());
        assert_eq!(
            manifest_policy(&context, realm_id, node_id, &manifest)
                .await
                .unwrap(),
            (None, None)
        );
    }

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
            compute_handle: None,
        };
        let document_id = ulid::Ulid::generate();
        let event_id = ulid::Ulid::generate();

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
