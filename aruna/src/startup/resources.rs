//! Acquires the long-lived node resources in order and releases them if a later step fails.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use aruna_api::monitoring::{MonitoringState, Readiness, serve_ops};
use aruna_blob::blob::{BackendRegistry, BlobHandle, BlobHandler};
use aruna_core::egress::EgressPolicy;
use aruna_core::metrics::NodeMetrics;
use aruna_core::shutdown::Shutdown;
use aruna_net::{NetConfig, NetHandle};
use aruna_operations::driver::DriverContext;
use aruna_operations::jobs::runtime::JobsRuntime;
use aruna_operations::metadata::{MetadataHandle, MetadataHandleOptions};
use aruna_operations::node::startup::RecoveryStatus;
use aruna_operations::sync::incoming::initialize_net_holder;
use aruna_operations::tasks::incoming::{TaskQueues, install_task_queues};
use aruna_tasks::TaskHandle;
use tokio::net::TcpListener;
use tracing::{error, info, warn};

use crate::compute_setup::build_registry;
use crate::config::{Config, open_storage, resolve_config};
use crate::settings::Settings;
use crate::shutdown::{NodeShutdown, ShutdownOutcome, shutdown_grace_env};

/// The node resources a running process owns. This is not the Tokio runtime.
pub struct NodeResources {
    pub(crate) config: Config,
    pub(crate) driver_ctx: Arc<DriverContext>,
    pub(crate) net_handle: NetHandle,
    pub(crate) shutdown: Shutdown,
    pub(crate) metrics: Arc<NodeMetrics>,
    pub(crate) readiness: Readiness,
    pub(crate) recovery: RecoveryStatus,
    pub(crate) jobs_runtime: Arc<JobsRuntime>,
    pub(crate) task_handle: TaskHandle,
    pub(crate) task_queues: TaskQueues,
    pub(crate) usage_counters_rebuilt: bool,
    /// The Docker session bridge gateway resolved with the typed compute
    /// settings; listener assembly consumes this instead of rereading the
    /// environment.
    pub(crate) session_s3: Option<std::net::SocketAddr>,
    pub(crate) monitoring: Arc<MonitoringState>,
    pub(crate) ops_handle: tokio::task::JoinHandle<()>,
}

/// The resources acquired so far. Infallible ownership is created up front;
/// every fallible acquisition assigns its resource before the next step runs.
struct Acquired {
    storage_handle: aruna_storage::StorageHandle,
    task_handle: TaskHandle,
    jobs_runtime: Arc<JobsRuntime>,
    shutdown: Shutdown,
    metrics: Arc<NodeMetrics>,
    readiness: Readiness,
    recovery: RecoveryStatus,
    net_handle: Option<NetHandle>,
    metadata_handle: Option<MetadataHandle>,
    blob_handle: Option<BlobHandle>,
    driver_ctx: Option<Arc<DriverContext>>,
    monitoring: Option<Arc<MonitoringState>>,
    ops_handle: Option<tokio::task::JoinHandle<()>>,
    task_queues: Option<TaskQueues>,
    usage_counters_rebuilt: bool,
    session_s3: Option<std::net::SocketAddr>,
}

impl Acquired {
    fn new(storage_handle: aruna_storage::StorageHandle, task_handle: TaskHandle) -> Self {
        Self {
            storage_handle,
            task_handle,
            jobs_runtime: JobsRuntime::new_paused(),
            shutdown: Shutdown::new(),
            metrics: Arc::new(NodeMetrics::new()),
            readiness: Readiness::new(),
            recovery: RecoveryStatus::new(),
            net_handle: None,
            metadata_handle: None,
            blob_handle: None,
            driver_ctx: None,
            monitoring: None,
            ops_handle: None,
            task_queues: None,
            usage_counters_rebuilt: false,
            session_s3: None,
        }
    }

    /// Runs the ordered teardown for the acquired subset: admissions, tasks,
    /// jobs, background, network, metadata, blob, storage. The ops task and
    /// driver context follow it; only a complete outcome reports a clean release.
    pub(crate) async fn cleanup(self, grace: Duration) -> ShutdownOutcome {
        info!("Startup stopped early; releasing the acquired resources");
        let ops = self.ops_handle;
        let driver_ctx = self.driver_ctx;
        let outcome = NodeShutdown {
            shutdown: self.shutdown,
            readiness: self.readiness,
            rest: None,
            s3: None,
            portal: None,
            session_s3: None,
            monitoring: self.monitoring,
            task_handle: self.task_handle,
            jobs_runtime: self.jobs_runtime,
            net_handle: self.net_handle,
            metadata_handle: self.metadata_handle,
            blob_handle: self.blob_handle,
            storage_handle: self.storage_handle,
            ops: None,
            grace,
        }
        .run()
        .await;
        if let Some(ops) = ops {
            ops.abort();
            let _ = ops.await;
        }
        drop(driver_ctx);
        outcome
    }

    fn finish(self, config: Config) -> NodeResources {
        NodeResources {
            config,
            driver_ctx: self
                .driver_ctx
                .expect("acquisition builds the driver context"),
            net_handle: self
                .net_handle
                .expect("acquisition builds the network handle first"),
            shutdown: self.shutdown,
            metrics: self.metrics,
            readiness: self.readiness,
            recovery: self.recovery,
            jobs_runtime: self.jobs_runtime,
            task_handle: self.task_handle,
            task_queues: self
                .task_queues
                .expect("acquisition builds the task queues"),
            usage_counters_rebuilt: self.usage_counters_rebuilt,
            session_s3: self.session_s3,
            monitoring: self
                .monitoring
                .expect("acquisition builds the monitoring state"),
            ops_handle: self.ops_handle.expect("acquisition starts the ops server"),
        }
    }
}

/// One acquired stage boundary. A failure reported here is handled exactly
/// like a failure inside the following acquisition, so startup tests can walk
/// every partial state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StartupStage {
    Net,
    Metadata,
    Blob,
    Compute,
    Ops,
    UsageCounters,
    TaskQueues,
}

/// A startup stop was accepted between acquisition stages. It is a distinct
/// outcome from a failure: the acquired subset is released and the node
/// reports a clean startup cancellation.
#[derive(Debug)]
pub(crate) struct StartupStopped;

impl std::fmt::Display for StartupStopped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("startup stop accepted during resource acquisition")
    }
}

impl std::error::Error for StartupStopped {}

/// A startup stop was accepted, but releasing the acquired subset did not
/// complete: at least one owner or persistence step stayed unresolved, so this
/// must not be reported as a clean startup cancellation.
#[derive(Debug)]
pub(crate) struct StartupCleanupIncomplete {
    outcome: ShutdownOutcome,
}

impl std::fmt::Display for StartupCleanupIncomplete {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "startup cleanup did not release every acquired owner: {:?}",
            self.outcome
        )
    }
}

impl std::error::Error for StartupCleanupIncomplete {}

/// Maps the cleanup of an accepted startup stop: only a sequence that released
/// every owner and persistence step is a clean cancellation; an incomplete one
/// becomes a typed startup error.
fn cleanup_stop_outcome(
    outcome: ShutdownOutcome,
) -> Result<Option<NodeResources>, Box<dyn std::error::Error>> {
    if outcome.complete() {
        Ok(None)
    } else {
        Err(Box::new(StartupCleanupIncomplete { outcome }))
    }
}

pub(crate) async fn acquire(
    stop: &tokio_util::sync::CancellationToken,
    on_drain: &mut impl FnMut(),
) -> Result<Option<NodeResources>, Box<dyn std::error::Error>> {
    // A pre-cancelled startup must not open the store or touch identity.
    if stop.is_cancelled() {
        return Ok(None);
    }
    // Parsing operator settings is pure; the store is the first owned
    // resource, established before identity and enrollment I/O.
    let settings = crate::settings::read_settings()?;
    let storage_handle = open_storage(&settings)?;
    // The node runtime always exists here; a missing one is a concrete startup
    // error, never a scheduler-less handle that only looks started.
    let task_handle = TaskHandle::try_new().map_err(std::io::Error::other)?;
    acquire_with_storage(
        settings,
        storage_handle,
        task_handle,
        stop,
        |_| Ok(()),
        on_drain,
    )
    .await
}

/// Acquires the node resources from parsed settings and an already-open store,
/// so later stages share one cleanup boundary. `Ok(None)` is a complete release,
/// a partial one is a typed error; `on_drain` arms escalation before each drain.
pub(crate) async fn acquire_with_storage(
    settings: Settings,
    storage_handle: aruna_storage::StorageHandle,
    task_handle: TaskHandle,
    stop: &tokio_util::sync::CancellationToken,
    mut checkpoint: impl FnMut(StartupStage) -> Result<(), Box<dyn std::error::Error>>,
    on_drain: &mut impl FnMut(),
) -> Result<Option<NodeResources>, Box<dyn std::error::Error>> {
    let mut acquired = Acquired::new(storage_handle, task_handle);
    // A stop accepted before configuration resolution must not read or write
    // identity or send enrollment traffic.
    if stop.is_cancelled() {
        on_drain();
        return cleanup_stop_outcome(acquired.cleanup(shutdown_grace_env()).await);
    }
    let config = match resolve_config(settings, acquired.storage_handle.clone(), stop).await {
        Ok(Some(config)) => config,
        Ok(None) => {
            on_drain();
            return cleanup_stop_outcome(acquired.cleanup(shutdown_grace_env()).await);
        }
        Err(error) => {
            on_drain();
            let cleanup = acquired.cleanup(shutdown_grace_env()).await;
            if !cleanup.complete() {
                warn!(
                    ?cleanup,
                    "Startup cleanup after a configuration failure did not release every owner"
                );
            }
            return Err(error.into());
        }
    };
    match fill(&config, &mut acquired, stop, &mut checkpoint).await {
        Ok(()) => Ok(Some(acquired.finish(config))),
        Err(error) => {
            on_drain();
            let cleanup = acquired.cleanup(shutdown_grace_env()).await;
            match error.downcast::<StartupStopped>() {
                Ok(_) => cleanup_stop_outcome(cleanup),
                Err(error) => {
                    if !cleanup.complete() {
                        warn!(
                            ?cleanup,
                            "Startup cleanup after an acquisition failure did not release every owner"
                        );
                    }
                    Err(error)
                }
            }
        }
    }
}

/// Acquires the resources for an already-resolved configuration. Kept for the
/// stage-walking startup tests; production resolves the configuration inside
/// the owned boundary via [`acquire_with_storage`].
#[cfg(test)]
pub(crate) async fn acquire_resources(
    config: Config,
    storage_handle: aruna_storage::StorageHandle,
    task_handle: TaskHandle,
    mut checkpoint: impl FnMut(StartupStage) -> Result<(), Box<dyn std::error::Error>>,
) -> Result<NodeResources, Box<dyn std::error::Error>> {
    let mut acquired = Acquired::new(storage_handle, task_handle);
    let stop = tokio_util::sync::CancellationToken::new();
    match fill(&config, &mut acquired, &stop, &mut checkpoint).await {
        Ok(()) => Ok(acquired.finish(config)),
        Err(error) => {
            acquired.cleanup(shutdown_grace_env()).await;
            Err(error)
        }
    }
}

/// Acquires every fallible resource in order, assigning each one to `acquired`
/// as soon as it exists so a later failure owns it.
async fn fill(
    config: &Config,
    acquired: &mut Acquired,
    stop: &tokio_util::sync::CancellationToken,
    checkpoint: &mut impl FnMut(StartupStage) -> Result<(), Box<dyn std::error::Error>>,
) -> Result<(), Box<dyn std::error::Error>> {
    // A stop accepted between stages must not admit the next acquisition.
    let stopped = |stop: &tokio_util::sync::CancellationToken| {
        if stop.is_cancelled() {
            Err(Box::new(StartupStopped) as Box<dyn std::error::Error>)
        } else {
            Ok(())
        }
    };
    stopped(stop)?;
    if matches!(
        config.startup_mode,
        crate::config::StartupMode::JoinRealm {
            phase: aruna_core::onboarding::OnboardingPhase::Bootstrapped
        }
    ) {
        let ticket = config
            .node_state
            .onboarding_sync_ticket
            .as_deref()
            .ok_or("missing onboarding sync ticket")?;
        let ticket = aruna_core::onboarding::OnboardingTicket::decode(ticket)?;
        let context = DriverContext {
            storage_handle: acquired.storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        aruna_operations::groups::deletion::install_onboarding(
            &context,
            &ticket,
            config.realm_id,
            config.node_id,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs(),
        )
        .await?;
        stopped(stop)?;
    }
    let net_handle = NetHandle::new(
        NetConfig {
            bind_addr: config.p2p_addr,
            secret_key: Some(config.net_secret_key.clone()),
            realm_id: config.realm_id,
            peer_nodes: config.peer_nodes.clone(),
            peer_endpoints: config.peer_endpoints.clone(),
            temporary_bootstrap_active: config.temporary_bootstrap_active,
            discovery_method: config.discovery_method.clone(),
            relay_method: config.relay_method.clone(),
            max_uni_streams: config.max_uni_streams,
            max_bidi_streams: config.max_bidi_streams,
            sync_storage_path: Some(config.sync_storage_path.clone()),
            document_sync_runtime: Some(config.document_sync_runtime),
            fjall_persist_policy: config.fjall_persist_policy,
        },
        acquired.storage_handle.clone(),
    )
    .await?;
    if let Err(error) = net_handle.reload_realm_peers().await {
        warn!(error = %error, "Failed to refresh realm peers from persisted config during startup");
    }
    acquired.net_handle = Some(net_handle.clone());
    checkpoint(StartupStage::Net)?;
    stopped(stop)?;

    let metadata_handle = MetadataHandle::new_with_options(
        &config.metadata_storage_path,
        config.node_id,
        acquired.storage_handle.clone(),
        Some(net_handle.clone()),
        Some(net_handle.document_sync_node()),
        Some(net_handle.document_sync_database()),
        MetadataHandleOptions::default()
            .with_search_storage(config.metadata_search_storage)
            .with_sync_policy(config.fjall_persist_policy),
    )?;
    acquired.metadata_handle = Some(metadata_handle.clone());
    checkpoint(StartupStage::Metadata)?;
    stopped(stop)?;

    let blob_handle = BlobHandler::with_registry(
        BackendRegistry::from_config(&config.blob_backends).map_err(std::io::Error::other)?,
        acquired.storage_handle.clone(),
        net_handle.clone(),
        EgressPolicy::strict().with_deny(config.blob_backends.extra_deny.clone()),
    )
    .await?;
    acquired.blob_handle = Some(blob_handle.clone());
    checkpoint(StartupStage::Blob)?;
    stopped(stop)?;

    // Compute settings are collected once from the explicit process source at
    // the resource boundary, then passed into backend construction.
    let compute_settings = crate::compute_setup::collect(&crate::settings::ProcessEnv)
        .map_err(std::io::Error::other)?;
    let compute = build_registry(config, &compute_settings)
        .await
        .map_err(std::io::Error::other)?;
    acquired.session_s3 = compute.session_s3;
    let compute_handle = compute.registry;
    checkpoint(StartupStage::Compute)?;
    stopped(stop)?;

    let driver_ctx = Arc::new(DriverContext {
        storage_handle: acquired.storage_handle.clone(),
        net_handle: Some(net_handle),
        blob_handle: Some(blob_handle),
        metadata_handle: Some(metadata_handle),
        task_handle: Some(acquired.task_handle.clone()),
        compute_handle: compute_handle.clone(),
    });
    // The driver context and the monitoring refresher it feeds are owned before
    // the ops listener can fail to bind.
    acquired.driver_ctx = Some(driver_ctx.clone());

    // Start ops before realm bootstrap so readiness reports startup failure.
    let ops_state = MonitoringState::with_recovery(
        driver_ctx.clone(),
        acquired.metrics.clone(),
        acquired.readiness.clone(),
        acquired.recovery.clone(),
    )
    .await;
    acquired.monitoring = Some(ops_state.clone());
    let ops_listener = TcpListener::bind(config.ops_socket_addr).await?;
    let bound = ops_listener.local_addr()?;
    let ops_handle = tokio::spawn(async move {
        if let Err(error) = serve_ops(ops_listener, ops_state).await {
            error!(error = %error, "Ops server stopped");
        }
    });
    info!(ops_address = %bound, "Ops server listening");
    acquired.ops_handle = Some(ops_handle);
    checkpoint(StartupStage::Ops)?;
    stopped(stop)?;

    // A rebuild is the only local evidence that counters were not carried over.
    acquired.usage_counters_rebuilt = ensure_usage_counters(driver_ctx.as_ref()).await?;
    checkpoint(StartupStage::UsageCounters)?;
    stopped(stop)?;

    // Bind compute reconciliation before startup recovery.
    initialize_net_holder(
        driver_ctx.clone(),
        config.rocrate_limits.clone(),
        acquired.jobs_runtime.clone(),
        &acquired.shutdown,
    );
    let task_queues = install_task_queues(
        driver_ctx.clone(),
        acquired.task_handle.clone(),
        acquired.jobs_runtime.clone(),
        config.rocrate_limits.clone(),
    )
    .await;
    acquired.task_queues = Some(task_queues);
    checkpoint(StartupStage::TaskQueues)?;

    Ok(())
}

/// Ensures the maintained usage counter shards exist before background writes
/// start, and reports whether that required a rebuild.
async fn ensure_usage_counters(
    driver_ctx: &DriverContext,
) -> Result<bool, Box<dyn std::error::Error>> {
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::USAGE_STATS_KEYSPACE;
    use aruna_core::structs::storage::usage::global_shard_keys;
    use aruna_operations::driver::drive;
    use aruna_operations::node::usage_stats::RebuildStatsOperation;

    let shard_keys = global_shard_keys();
    let event = driver_ctx
        .storage_handle
        .send_storage_effect(StorageEffect::BatchRead {
            reads: shard_keys
                .iter()
                .map(|key| (USAGE_STATS_KEYSPACE.to_string(), key.clone().into()))
                .collect(),
            txn_id: None,
        })
        .await;

    match event {
        Event::Storage(StorageEvent::BatchReadResult { values }) => {
            if values.len() != shard_keys.len() {
                return Err(format!(
                    "usage counter probe returned {} values for {} shards",
                    values.len(),
                    shard_keys.len()
                )
                .into());
            }
            if values.iter().any(|(_, value)| value.is_none()) {
                drive(RebuildStatsOperation::new(), driver_ctx).await?;
                return Ok(true);
            }
        }
        Event::Storage(StorageEvent::Error { error }) => {
            return Err(format!("usage counter probe failed: {error}").into());
        }
        other => {
            return Err(format!("usage counter probe received unexpected event: {other:?}").into());
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shutdown::MIN_SHUTDOWN_GRACE;
    use aruna_core::effects::StorageEffect;
    use aruna_core::errors::StorageError;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::USAGE_STATS_KEYSPACE;
    use aruna_core::structs::storage::usage::{UsageCounters, global_shard_keys};
    use aruna_storage::StorageHandle;
    use std::thread;
    use tempfile::tempdir;

    fn test_driver_ctx(storage_handle: StorageHandle) -> DriverContext {
        DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        }
    }

    fn open_storage(dir: &tempfile::TempDir) -> StorageHandle {
        aruna_storage::FjallStorage::open(dir.path().to_str().expect("utf8 path"))
            .expect("storage opens")
    }

    #[tokio::test]
    async fn counters_rebuild_shards() {
        let temp = tempdir().expect("temp dir");
        let storage_handle = open_storage(&temp);
        let driver_ctx = test_driver_ctx(storage_handle.clone());

        assert!(
            ensure_usage_counters(&driver_ctx).await.unwrap(),
            "missing shards must report a rebuild"
        );
        assert!(
            !ensure_usage_counters(&driver_ctx).await.unwrap(),
            "intact shards must report no rebuild"
        );

        for key in global_shard_keys() {
            let event = storage_handle
                .send_storage_effect(StorageEffect::Read {
                    key_space: USAGE_STATS_KEYSPACE.to_string(),
                    key: key.into(),
                    txn_id: None,
                })
                .await;
            let Event::Storage(StorageEvent::ReadResult {
                value: Some(bytes), ..
            }) = event
            else {
                panic!("expected rebuilt usage shard, got {event:?}");
            };
            assert_eq!(
                UsageCounters::from_bytes(bytes.as_ref()).unwrap(),
                UsageCounters::default()
            );
        }
    }

    #[tokio::test]
    async fn counters_return_errors() {
        let (storage_handle, receivers) = StorageHandle::new();
        let receiver = receivers.foreground;
        let worker = thread::spawn(move || {
            let (effect, response_tx, _span, _queued_at, _in_flight) = receiver
                .recv()
                .expect("usage counter ensure should probe storage");
            assert!(matches!(effect, StorageEffect::BatchRead { .. }));
            response_tx.send(StorageEvent::Error {
                error: StorageError::ReadError("boom".to_string()),
            });
        });
        let driver_ctx = test_driver_ctx(storage_handle);

        let error = ensure_usage_counters(&driver_ctx)
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("usage counter probe failed"));
        assert!(error.contains("boom"), "storage cause missing: {error}");
        worker.join().expect("storage responder should finish");
    }

    #[tokio::test]
    async fn counters_reject_events() {
        let (storage_handle, receivers) = StorageHandle::new();
        let receiver = receivers.foreground;
        let worker = thread::spawn(move || {
            let (effect, response_tx, _span, _queued_at, _in_flight) = receiver
                .recv()
                .expect("usage counter ensure should probe storage");
            assert!(matches!(effect, StorageEffect::BatchRead { .. }));
            response_tx.send(StorageEvent::SyncAllFinished);
        });
        let driver_ctx = test_driver_ctx(storage_handle);

        let error = ensure_usage_counters(&driver_ctx)
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("usage counter probe received unexpected event"));
        worker.join().expect("storage responder should finish");
    }

    // A failure with nothing fallible acquired still releases storage; later
    // acquisitions stay absent rather than faked. The smallest supported grace
    // funds writer phases rather than racing their acknowledgements against a zero budget.
    #[tokio::test]
    async fn cleanup_releases_subset() {
        let temp = tempdir().expect("temp dir");
        let storage_handle = open_storage(&temp);
        let task_handle = TaskHandle::new();
        let acquired = Acquired::new(storage_handle.clone(), task_handle);
        assert!(acquired.net_handle.is_none());
        assert!(acquired.metadata_handle.is_none());
        assert!(acquired.ops_handle.is_none());

        let outcome = acquired.cleanup(MIN_SHUTDOWN_GRACE).await;
        assert!(
            outcome.complete(),
            "an idle acquired subset must release cleanly: {outcome:?}"
        );

        let event = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: "late".to_string(),
                key: b"key".to_vec().into(),
                value: b"value".to_vec().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::Error {
                error: StorageError::Closed
            })
        ));
    }

    // A tracked child that ignores cancellation makes the cleanup outcome
    // incomplete even though the store is still closed behind the bounded
    // drain; the caller must not report a clean startup cancellation.
    #[tokio::test]
    async fn cleanup_reports_incomplete() {
        let temp = tempdir().expect("temp dir");
        let storage_handle = open_storage(&temp);
        let task_handle = TaskHandle::new();
        let acquired = Acquired::new(storage_handle.clone(), task_handle);
        acquired.shutdown.spawn(std::future::pending());
        assert_eq!(acquired.shutdown.tracked_children(), 1);

        let outcome = acquired.cleanup(Duration::from_millis(200)).await;

        assert!(
            !outcome.background_drained,
            "a pending tracked child must not be reported drained: {outcome:?}"
        );
        assert!(!outcome.complete());

        let event = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: "late".to_string(),
                key: b"key".to_vec().into(),
                value: b"value".to_vec().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::Error {
                error: StorageError::Closed
            })
        ));
    }

    // A grace below `MIN_SHUTDOWN_GRACE` cannot fund the protected tail, so the
    // writer phases get no budget. A held tracked child keeps the outcome
    // incomplete; it is released before the test returns.
    #[tokio::test]
    async fn budget_exhausted_incomplete() {
        let temp = tempdir().expect("temp dir");
        let storage_handle = open_storage(&temp);
        let task_handle = TaskHandle::new();
        let acquired = Acquired::new(storage_handle.clone(), task_handle);
        // Documented too-small grace: one second instead of the supported
        // minimum, so no writer phase has a usable budget.
        let too_small = Duration::from_secs(1);

        // The tracked child holds the background drain at its real join: it
        // reports that it started and finishes only once the test releases it.
        let (started, started_rx) = tokio::sync::oneshot::channel();
        let (release, release_rx) = tokio::sync::oneshot::channel::<()>();
        let (finished, finished_rx) = tokio::sync::oneshot::channel();
        acquired.shutdown.spawn(async move {
            let _ = started.send(());
            let _ = release_rx.await;
            let _ = finished.send(());
        });
        tokio::time::timeout(Duration::from_secs(5), started_rx)
            .await
            .expect("the boundary child must start")
            .expect("the boundary child must signal");

        let outcome = acquired.cleanup(too_small).await;

        assert!(
            !outcome.background_drained,
            "a held background child must not be reported drained: {outcome:?}"
        );
        assert!(
            !outcome.complete(),
            "an exhausted writer budget must not report a clean release: {outcome:?}"
        );

        let event = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: "late".to_string(),
                key: b"key".to_vec().into(),
                value: b"value".to_vec().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::Error {
                error: StorageError::Closed
            })
        ));

        release
            .send(())
            .expect("the boundary child must still wait");
        tokio::time::timeout(Duration::from_secs(5), finished_rx)
            .await
            .expect("the released boundary child must finish")
            .expect("the boundary child must report completion");
    }

    // A failure after any acquisition stage must release exactly the acquired
    // subset: every later stage is absent and the store lock is given back.
    #[tokio::test]
    async fn stage_failure_releases() {
        use crate::config::resolve_settings;
        use crate::settings::read_settings_from;

        let stages = [
            StartupStage::Net,
            StartupStage::Metadata,
            StartupStage::Blob,
            StartupStage::Compute,
            StartupStage::Ops,
            StartupStage::UsageCounters,
            StartupStage::TaskQueues,
        ];
        for stage in stages {
            let temp = tempdir().expect("temp dir");
            let path = temp.path().to_str().expect("utf8 path").to_string();
            let map: std::collections::BTreeMap<String, String> = [
                ("STORAGE_PATH".to_string(), path.clone()),
                ("SOCKET_ADDRESS".to_string(), "127.0.0.1:0".to_string()),
                ("P2P_SOCKET_ADDRESS".to_string(), "127.0.0.1:0".to_string()),
                ("S3_HOST".to_string(), "127.0.0.1:0".to_string()),
                ("S3_ADDRESS".to_string(), "127.0.0.1:0".to_string()),
                ("PORTAL_MODE".to_string(), "disabled".to_string()),
                ("ARUNA_FJALL_PERSIST_MODE".to_string(), "buffer".to_string()),
            ]
            .into_iter()
            .collect();

            let (config, storage) = resolve_settings(read_settings_from(&map).unwrap())
                .await
                .expect("test settings resolve");
            let task_handle = TaskHandle::new();
            let error = match acquire_resources(config, storage, task_handle, |observed| {
                if observed == stage {
                    Err(format!("injected failure after {stage:?}").into())
                } else {
                    Ok(())
                }
            })
            .await
            {
                Ok(_) => panic!("the injected stage failure must stop acquisition"),
                Err(error) => error,
            };
            assert!(
                error.to_string().contains("injected failure"),
                "{stage:?}: {error}"
            );

            // Cleanup released every store handle: reopening the same root works.
            let reopened = aruna_storage::FjallStorage::open(&path);
            assert!(
                reopened.is_ok(),
                "{stage:?}: storage stayed locked after cleanup: {:?}",
                reopened.err()
            );
        }
    }

    // A pre-cancelled startup must release the store without loading,
    // generating, or persisting an identity or sending enrollment traffic.
    #[tokio::test]
    async fn cancelled_startup_skips() {
        use crate::settings::read_settings_from;

        let temp = tempdir().expect("temp dir");
        let path = temp.path().to_str().expect("utf8 path").to_string();
        let map: std::collections::BTreeMap<String, String> = [
            ("STORAGE_PATH".to_string(), path.clone()),
            ("SOCKET_ADDRESS".to_string(), "127.0.0.1:0".to_string()),
            ("P2P_SOCKET_ADDRESS".to_string(), "127.0.0.1:0".to_string()),
            ("S3_HOST".to_string(), "127.0.0.1:0".to_string()),
            ("S3_ADDRESS".to_string(), "127.0.0.1:0".to_string()),
            ("PORTAL_MODE".to_string(), "disabled".to_string()),
            ("ARUNA_FJALL_PERSIST_MODE".to_string(), "buffer".to_string()),
        ]
        .into_iter()
        .collect();
        let settings = read_settings_from(&map).expect("settings parse");
        let storage = crate::config::open_storage(&settings).expect("storage opens");
        let task_handle = TaskHandle::new();
        let stop = tokio_util::sync::CancellationToken::new();
        stop.cancel();

        let outcome = acquire_with_storage(
            settings,
            storage,
            task_handle,
            &stop,
            |_| Ok(()),
            &mut || {},
        )
        .await
        .expect("an accepted stop is not a failure");
        assert!(outcome.is_none());

        let reopened =
            aruna_storage::FjallStorage::open(&path).expect("cleanup released the store");
        let identity = crate::identity::IdentityStore::from_storage(reopened);
        assert!(
            identity.load().await.unwrap().is_none(),
            "a pre-cancelled startup must not generate or persist an identity"
        );
    }
}
