//! Acquires the long-lived node resources in order and releases them if a later step fails.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use crate::inbound::initialize_net_holder;
use crate::tasks::incoming::TaskQueues;
use crate::tasks::incoming::restore::install_with_retention;
use aruna_api::monitoring::{MonitoringState, Readiness, serve_ops};
use aruna_blob::blob::registry::BackendRegistry;
use aruna_blob::blob::{BlobHandle, BlobHandler};
use aruna_core::egress::EgressPolicy;
use aruna_core::id::NodeId;
use aruna_core::metrics::NodeMetrics;
use aruna_core::structs::identity::realm::RealmId;
use aruna_ops_data::metadata::projection::{DEFAULT_SKEW_MS, store_clock_skew};
use aruna_ops_data::metadata::provenance::export::ForgetSpoolOperation;
use aruna_ops_data::s3::usage_rebuild::RebuildStatsOperation;
use aruna_ops_data::transfer::sharding::begin_range_rebuilds;
use aruna_ops_data::usage_publish::PublishUsageOperation;
use aruna_ops_identity::groups::deletion::coordinator::InstallOnboardingOperation;
use aruna_ops_identity::shard::recovery::RecoveryStatus;
use aruna_ops_work::node::counter_check::CounterCheckOperation;
use aruna_p2p::NetHandle;
use aruna_p2p::config::NetConfig;
use aruna_rdf_graph::{GraphHandle, GraphOptions};
use aruna_runner::driver::{DriverContext, drive};
use aruna_runner::jobs::runtime::JobsRuntime;
use aruna_scheduler::TaskHandle;
use aruna_scheduler::shutdown::Shutdown;
use aruna_storage::errors::StorageLibError;
use aruna_storage::storage::handle::StorageHandle;
use aruna_storage::storage::persistence::FjallPersistPolicy;
use aruna_storage::storage::worker::FjallStorage;
use aruna_vfs::{VfsConfig, VfsHandle};
use irokle::net::IrohRuntimeConfig;
use tokio::net::TcpListener;
use tracing::{error, info, warn};

use crate::compute_setup::build_registry;
use crate::shutdown::{NodeShutdown, ShutdownOutcome, shutdown_grace_env};
use aruna_config::settings::{DocumentSyncSettings, Settings, VfsSettings};
use aruna_config::{Config, SetupError, resolve_config};

/// How much event recording keeps in memory when `EVENT_RECORDING` is enabled.
const RECORD_LIMITS: aruna_runner::recorder::RecordLimits = aruna_runner::recorder::RecordLimits {
    total_bytes: 64 << 20,
    entry_bytes: 16 << 10,
};
/// Pause before a failed background usage rebuild starts again from a new snapshot.
const REBUILD_RETRY_PAUSE: Duration = Duration::from_secs(30);

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
    /// The SMB and NFS settings, which the resolved configuration does not carry.
    pub(crate) vfs: VfsSettings,
}

/// The resources acquired so far. Infallible ownership is created up front;
/// every fallible acquisition assigns its resource before the next step runs.
struct Acquired {
    storage_handle: aruna_storage::storage::handle::StorageHandle,
    task_handle: TaskHandle,
    jobs_runtime: Arc<JobsRuntime>,
    shutdown: Shutdown,
    metrics: Arc<NodeMetrics>,
    readiness: Readiness,
    recovery: RecoveryStatus,
    net_handle: Option<NetHandle>,
    graph_handle: Option<GraphHandle>,
    blob_handle: Option<BlobHandle>,
    driver_ctx: Option<Arc<DriverContext>>,
    monitoring: Option<Arc<MonitoringState>>,
    ops_handle: Option<tokio::task::JoinHandle<()>>,
    task_queues: Option<TaskQueues>,
    usage_counters_rebuilt: bool,
    session_s3: Option<std::net::SocketAddr>,
}

impl Acquired {
    fn new(
        storage_handle: aruna_storage::storage::handle::StorageHandle,
        task_handle: TaskHandle,
    ) -> Self {
        let jobs_runtime = JobsRuntime::new_paused();
        Self {
            storage_handle,
            task_handle,
            jobs_runtime,
            shutdown: Shutdown::new(),
            metrics: Arc::new(NodeMetrics::new()),
            readiness: Readiness::new(),
            recovery: RecoveryStatus::new(),
            net_handle: None,
            graph_handle: None,
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
        // Startup may stop before the context exists; leases are then handed back over storage.
        let driver_ctx = self
            .driver_ctx
            .unwrap_or_else(|| Arc::new(DriverContext::storage_only(self.storage_handle.clone())));
        let outcome = NodeShutdown {
            shutdown: self.shutdown,
            readiness: self.readiness,
            rest: None,
            s3: None,
            portal: None,
            session_s3: None,
            smb: None,
            nfs: None,
            https: None,
            monitoring: self.monitoring,
            task_handle: self.task_handle,
            jobs_runtime: self.jobs_runtime,
            driver_ctx: driver_ctx.clone(),
            net_handle: self.net_handle,
            graph_handle: self.graph_handle,
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

    fn finish(self, config: Config, vfs: VfsSettings) -> NodeResources {
        NodeResources {
            config,
            vfs,
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

/// A failure while opening the store or resolving the node configuration.
#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    #[error(transparent)]
    Storage(#[from] StorageLibError),
    #[error(transparent)]
    Setup(#[from] SetupError),
}

/// Opens the local store for parsed settings. The caller owns the handle from
/// here on, before any identity or enrollment I/O needs it.
pub fn open_storage(settings: &Settings) -> Result<StorageHandle, StorageLibError> {
    FjallStorage::open_with_cache(
        &settings.storage_path,
        settings.fjall_persist_policy,
        settings.fjall_cache_bytes,
    )
}

/// Convenience for callers that want the store and the resolved configuration
/// together. Startup uses [`open_storage`] and [`resolve_config`] directly so
/// the resource owner exists before the identity I/O.
pub async fn resolve_settings(settings: Settings) -> Result<(Config, StorageHandle), ResolveError> {
    let storage_handle = open_storage(&settings)?;
    let stop = tokio_util::sync::CancellationToken::new();
    let config = resolve_config(settings, storage_handle.clone(), &stop)
        .await?
        .expect("a resolution with a fresh stop token always resolves");
    Ok((config, storage_handle))
}

/// The document sync timings of the net adapter; an unset timing keeps the Irokle default.
pub fn sync_runtime(settings: DocumentSyncSettings) -> IrohRuntimeConfig {
    let default = IrohRuntimeConfig::default();
    IrohRuntimeConfig {
        connect_timeout: settings.connect_timeout.unwrap_or(default.connect_timeout),
        sync_io_timeout: settings.sync_io_timeout.unwrap_or(default.sync_io_timeout),
        resync_interval: settings.resync_interval.unwrap_or(default.resync_interval),
        resync_initial_backoff: settings
            .resync_initial_backoff
            .unwrap_or(default.resync_initial_backoff),
        resync_max_backoff: settings
            .resync_max_backoff
            .unwrap_or(default.resync_max_backoff),
        full_sweep_interval: settings
            .full_sweep_interval
            .unwrap_or(default.full_sweep_interval),
        full_sweep_time_of_day: settings
            .full_sweep_time
            .unwrap_or(default.full_sweep_time_of_day),
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
    let settings = aruna_config::settings::read_settings()?;
    if settings.event_recording {
        aruna_runner::recorder::enable(RECORD_LIMITS);
    }
    aruna_runner::driver::set_hang_limits(settings.hang_limits);
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
    storage_handle: aruna_storage::storage::handle::StorageHandle,
    task_handle: TaskHandle,
    stop: &tokio_util::sync::CancellationToken,
    mut checkpoint: impl FnMut(StartupStage) -> Result<(), Box<dyn std::error::Error>>,
    on_drain: &mut impl FnMut(),
) -> Result<Option<NodeResources>, Box<dyn std::error::Error>> {
    let mut acquired = Acquired::new(storage_handle, task_handle);
    let vfs = settings.vfs.clone();
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
    match fill(&config, &vfs, &mut acquired, stop, &mut checkpoint).await {
        Ok(()) => Ok(Some(acquired.finish(config, vfs))),
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
    storage_handle: aruna_storage::storage::handle::StorageHandle,
    task_handle: TaskHandle,
    mut checkpoint: impl FnMut(StartupStage) -> Result<(), Box<dyn std::error::Error>>,
) -> Result<NodeResources, Box<dyn std::error::Error>> {
    let mut acquired = Acquired::new(storage_handle, task_handle);
    let stop = tokio_util::sync::CancellationToken::new();
    let vfs = VfsSettings::default();
    match fill(&config, &vfs, &mut acquired, &stop, &mut checkpoint).await {
        Ok(()) => Ok(acquired.finish(config, vfs)),
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
    vfs: &VfsSettings,
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
    // Secret rows are sealed with the node key before any operation reads or writes one.
    acquired.storage_handle.seal_secrets(
        aruna_core::credential_encryption::CredentialEncryptionKey::derive(
            &config.node_state.net_secret_key,
        ),
    );
    acquired
        .storage_handle
        .open_vault(aruna_core::node::vault::NodeVaultKey::derive(
            &config.node_state.net_secret_key,
        ));
    // Only holders of the realm signing key derive the realm keys; storage opens with them.
    if let aruna_core::structs::identity::auth::NodeCapabilities::Management {
        realm_signing_key,
        ..
    } = &config.node_capabilities
    {
        let keys = aruna_core::realm_vault::RealmKeys::derive(realm_signing_key);
        acquired.storage_handle.open_realm(keys);
    }
    // After every start no job work is admitted until the node caught up; the fence is durable
    // before any peer can reach this node.
    let fence = DriverContext::storage_only(acquired.storage_handle.clone());
    let raise =
        aruna_ops_work::jobs::fence::RaiseFenceOperation::new(config.node_id, config.realm_id);
    drive(raise, &fence)
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    if matches!(
        config.startup_mode,
        aruna_config::StartupMode::JoinRealm {
            phase: aruna_core::onboarding::OnboardingPhase::Bootstrapped
        }
    ) {
        let ticket = config
            .node_state
            .onboarding_sync_ticket
            .as_deref()
            .ok_or("missing onboarding sync ticket")?;
        let ticket = aruna_core::onboarding::OnboardingTicket::decode(ticket)?;
        let context = DriverContext::storage_only(acquired.storage_handle.clone());
        let now = aruna_clock::unix_timestamp_secs();
        let install =
            InstallOnboardingOperation::new(&ticket, config.realm_id, config.node_id, now);
        aruna_runner::driver::drive(install, &context).await?;
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
            document_sync_runtime: Some(sync_runtime(config.document_sync_runtime)),
            fjall_persist_policy: config.fjall_persist_policy,
        },
        acquired.storage_handle.clone(),
    )
    .await?;
    // Only the net adapter signs federation payloads, so the key never enters an operation.
    net_handle.install_federation_keys(config.node_capabilities.clone());
    if let Err(error) = net_handle.reload_realm_peers().await {
        warn!(error = %error, "Failed to refresh realm peers from persisted config during startup");
    }
    acquired.net_handle = Some(net_handle.clone());
    checkpoint(StartupStage::Net)?;
    stopped(stop)?;

    let graph_handle = GraphHandle::open(
        &config.metadata_storage_path,
        config.node_id,
        Some(net_handle.document_sync_node()),
        Some(net_handle.document_sync_database()),
        GraphOptions::default()
            .with_search_storage(config.metadata_search_storage)
            .with_sync_all(config.fjall_persist_policy == FjallPersistPolicy::SyncAll),
    )?;
    acquired.graph_handle = Some(graph_handle.clone());
    acquired
        .storage_handle
        .install_metadata_indexes(&config.metadata_storage_path);
    checkpoint(StartupStage::Metadata)?;
    stopped(stop)?;

    let blob_handle = BlobHandler::with_registry(
        BackendRegistry::from_config(&config.blob_backends).map_err(std::io::Error::other)?,
        acquired.storage_handle.clone(),
        net_handle.clone(),
        EgressPolicy::strict().with_deny(config.blob_backends.extra_deny.clone()),
        Some(config.spool_path.clone()),
    )
    .await?;
    acquired.blob_handle = Some(blob_handle.clone());
    checkpoint(StartupStage::Blob)?;
    stopped(stop)?;

    // Compute settings are collected once from the explicit process source at
    // the resource boundary, then passed into backend construction.
    let compute_settings = crate::compute_setup::collect(&aruna_config::settings::ProcessEnv)
        .map_err(std::io::Error::other)?;
    let compute = build_registry(config, &compute_settings)
        .await
        .map_err(std::io::Error::other)?;
    acquired.session_s3 = compute.session_s3;
    let compute_handle = compute.registry;
    checkpoint(StartupStage::Compute)?;
    stopped(stop)?;

    // Storage reports head changes to the file servers before any of them serves.
    let vfs_handle = open_vfs(vfs, config, &acquired.storage_handle)?;
    let (http, policy) = blob_handle.repository_client();
    let driver_ctx = Arc::new(DriverContext {
        storage_handle: acquired.storage_handle.clone(),
        net_handle: Some(net_handle),
        blob_handle: Some(blob_handle),
        graph_handle: Some(graph_handle),
        task_handle: Some(acquired.task_handle.clone()),
        compute_handle: compute_handle.clone(),
        outbound_handle: Some(aruna_egress::OutboundHandle::new(http, policy)),
        vfs_handle,
    });
    // The driver context and the monitoring refresher it feeds are owned before
    // the ops listener can fail to bind.
    acquired.driver_ctx = Some(driver_ctx.clone());
    // Projections read the skew setting from storage; it is stored before any of them runs.
    if let Err(error) = drive(store_clock_skew(clock_skew_env()), &driver_ctx).await {
        warn!(%error, "The clock skew setting could not be stored");
    }

    // Start ops before realm bootstrap so readiness reports startup failure.
    let ops_state = MonitoringState::with_recovery(
        driver_ctx.clone(),
        acquired.metrics.clone(),
        acquired.readiness.clone(),
        acquired.recovery.clone(),
    );
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

    // A rebuild is the only local evidence that counters were not carried over. It runs in
    // the background; quota-limited writes are refused until it finishes.
    acquired.usage_counters_rebuilt =
        drive(CounterCheckOperation::new(), driver_ctx.as_ref()).await?;
    if acquired.usage_counters_rebuilt {
        spawn_usage_rebuild(
            driver_ctx.clone(),
            config.realm_id,
            config.node_id,
            &acquired.shutdown,
        );
    }
    checkpoint(StartupStage::UsageCounters)?;
    stopped(stop)?;

    // Node-managed bucket keys open before content work; vault-locked keys stay locked.
    open_managed_keys(driver_ctx.as_ref(), config).await?;
    record_restart_locks(driver_ctx.as_ref(), config).await?;
    // Cached realm secrets the config no longer names go before any email job runs.
    let refresh = aruna_ops_work::notifications::email::RefreshMailOperation::new(config.realm_id);
    if let Err(error) = drive(refresh, &driver_ctx).await {
        warn!(%error, "Mail settings refresh stopped during startup");
    }
    // Exports cut off by the last stop left their working rows; no export runs before serving.
    if let Err(error) = drive(ForgetSpoolOperation::all(), &driver_ctx).await {
        warn!(%error, "Provenance working rows were not cleared during startup");
    }
    // Recovery that weakened while the node was down, or later through role changes, is told.
    let origin = (config.realm_id, config.node_id);
    crate::startup::sweeps::spawn_recovery_sweep(driver_ctx.clone(), origin, &acquired.shutdown);
    stopped(stop)?;
    // The file servers recover their durable state before any listener binds.
    if driver_ctx.vfs_handle.is_some() {
        recover_vfs(driver_ctx.as_ref(), origin).await?;
        stopped(stop)?;
    }

    // Bind compute reconciliation before startup recovery.
    initialize_net_holder(
        driver_ctx.clone(),
        config.rocrate_limits.clone(),
        acquired.jobs_runtime.clone(),
        &acquired.shutdown,
    );
    let retention = (
        vfs.tombstone_retention_secs.saturating_mul(1000),
        vfs.stage_retention_secs.saturating_mul(1000),
    );
    let task_queues = install_with_retention(
        driver_ctx.clone(),
        acquired.task_handle.clone(),
        acquired.jobs_runtime.clone(),
        config.rocrate_limits.clone(),
        retention,
    )
    .await;
    acquired.task_queues = Some(task_queues);
    checkpoint(StartupStage::TaskQueues)?;

    Ok(())
}

/// Opens the stage folder when SMB or NFS is on, or when an earlier run left it on disk, and
/// has storage report every committed head change to it. Recovery and vault fences then see
/// its scratch files with both listeners off. `None` only when both are off and it is absent.
fn open_vfs(
    vfs: &VfsSettings,
    config: &Config,
    storage_handle: &StorageHandle,
) -> Result<Option<VfsHandle>, Box<dyn std::error::Error>> {
    let stage_dir = vfs
        .stage_dir
        .clone()
        .unwrap_or_else(|| std::path::PathBuf::from(&config.storage_path).join("vfs-stage"));
    let present = stage_dir.try_exists().map_err(|error| {
        format!(
            "failed to inspect VFS stage directory {}: {error}",
            stage_dir.display()
        )
    })?;
    if !vfs.enabled() && !present {
        return Ok(None);
    }
    let vfs_handle = VfsHandle::open(VfsConfig {
        stage_dir,
        node_stage_bytes: vfs.node_stage_bytes,
        group_stage_bytes: vfs.group_stage_bytes,
    })?;
    if !storage_handle.install_head_observer(Arc::new(vfs_handle.clone())) {
        return Err("another head change observer is already installed on storage".into());
    }
    Ok(Some(vfs_handle))
}

/// Syncs a new boot generation and settles receipts, handles and stages from durable rows.
/// Referenced checkpoints that are missing or corrupt are storage failures that stop the start.
async fn recover_vfs(
    driver_ctx: &DriverContext,
    (realm_id, node_id): (aruna_core::structs::identity::realm::RealmId, NodeId),
) -> Result<(), Box<dyn std::error::Error>> {
    use aruna_ops_data::vfs::recovery::ColdStartOperation;

    let cold_start = ColdStartOperation::new().with_admission(realm_id, node_id);
    let report = drive(cold_start, driver_ctx).await?;
    if !report.corrupt.is_empty() {
        error!(
            stages = ?report.corrupt,
            "File server checkpoints are missing or corrupt; the node does not start"
        );
        let message = format!(
            "file server checkpoints are missing or corrupt: {:?}",
            report.corrupt
        );
        return Err(message.into());
    }
    info!(
        boot_generation = report.boot_generation,
        receipts = report.receipts,
        restored = report.restored,
        retained = report.retained,
        orphans = report.orphans.len(),
        "Recovered the file server state"
    );
    Ok(())
}

/// The largest clock skew a projection accepts, from `MAX_CLOCK_SKEW_SECS` in seconds.
fn clock_skew_env() -> u64 {
    let seconds = std::env::var("MAX_CLOCK_SKEW_SECS").ok();
    let seconds = seconds.and_then(|value| value.parse::<u64>().ok());
    seconds.map_or(DEFAULT_SKEW_MS, |seconds| seconds.saturating_mul(1000))
}

/// Opens the node-managed bucket keys, then resumes the work that waited for them. A key that
/// fails stays locked and is reported.
async fn open_managed_keys(
    driver_ctx: &DriverContext,
    config: &Config,
) -> Result<(), Box<dyn std::error::Error>> {
    use aruna_clock::unix_timestamp_millis;
    use aruna_ops_data::s3::bucket::key::startup::OpenManagedOperation;
    use aruna_ops_work::jobs::key_wake::WakeUnlockedOperation;

    let keys = drive(OpenManagedOperation::new(), driver_ctx).await?;
    for (key, reason) in &keys.failed {
        warn!(
            bucket_id = %key.bucket_id,
            generation = key.generation,
            reason = %reason,
            "A node-managed bucket key stays locked"
        );
    }
    if keys.unreadable > 0 {
        warn!(rows = keys.unreadable, "Skipped unreadable bucket key rows");
    }
    info!(
        opened = keys.opened.len(),
        "Opened node-managed bucket keys"
    );
    let origin = (config.realm_id, config.node_id);
    for key in keys.opened {
        let now_ms = unix_timestamp_millis();
        let limits = &config.rocrate_limits;
        let wake = WakeUnlockedOperation::new(key, now_ms, origin, limits);
        if let Err(error) = drive(wake, driver_ctx).await {
            warn!(
                bucket_id = %key.bucket_id,
                generation = key.generation,
                error = %error,
                "Failed to resume work waiting for a node-managed key"
            );
        }
    }
    Ok(())
}

/// Records the restart lock of vault-locked buckets that were unlocked before this start and
/// tells their holders.
async fn record_restart_locks(
    driver_ctx: &DriverContext,
    config: &Config,
) -> Result<(), Box<dyn std::error::Error>> {
    use aruna_ops_data::s3::bucket::audit_retry::ResumeAuditOperation;
    use aruna_ops_data::s3::bucket::key::restart::RestartScanOperation;
    use aruna_ops_data::s3::restart_notice::RestartNoticeOperation;

    // Records kept by retry timers before the stop join the trail before it is read.
    match drive(ResumeAuditOperation::default(), driver_ctx).await {
        Ok(0) => {}
        Ok(stored) => info!(records = stored, "Stored kept bucket key audit records"),
        Err(error) => warn!(error = %error, "Kept bucket key audit records not read"),
    }
    let now_ms = aruna_clock::unix_timestamp_millis();
    let scan = RestartScanOperation::new(now_ms, config.realm_id);
    let buckets = drive(scan, driver_ctx).await?;
    if buckets.is_empty() {
        return Ok(());
    }
    let count = buckets.len();
    let boot_id = ulid::Ulid::generate();
    drive(
        RestartNoticeOperation::new(config.node_id, boot_id, buckets),
        driver_ctx,
    )
    .await?;
    info!(
        buckets = count,
        "Recorded the restart lock of unlocked buckets"
    );
    Ok(())
}

/// Rebuilds the usage counters in the background until one rebuild finishes, then publishes
/// full usage snapshots. Startup does not wait; until then quota-limited writes are refused.
fn spawn_usage_rebuild(
    context: Arc<DriverContext>,
    realm_id: RealmId,
    node_id: NodeId,
    shutdown: &Shutdown,
) {
    let token = shutdown.token();
    shutdown.spawn(async move {
        // Range counters of distributed buckets are confirmed again by their own rebuilds.
        match drive(begin_range_rebuilds(), &context).await {
            Ok(begun) if begun > 0 => info!(ranges = begun, "Range counter rebuilds begun"),
            Ok(_) => {}
            Err(error) => warn!(%error, "Range counter rebuilds could not begin"),
        }
        loop {
            let rebuild = drive(RebuildStatsOperation::new().with_realm(realm_id), &context);
            let result = tokio::select! {
                _ = token.cancelled() => return,
                result = rebuild => result,
            };
            match result {
                Ok(_) => break,
                Err(error) => warn!(error = %error, "Usage counter rebuild failed; retrying"),
            }
            tokio::select! {
                _ = token.cancelled() => return,
                _ = tokio::time::sleep(REBUILD_RETRY_PAUSE) => {}
            }
        }
        info!("Usage counters rebuilt");
        let serving = context.node_and_realm().map(|(_, realm_id)| realm_id);
        let publish = PublishUsageOperation::new((node_id, realm_id), serving, true, true);
        if let Err(error) = drive(publish, context.as_ref()).await {
            warn!(error = %error, "Usage snapshots were not published after the rebuild");
        }
    });
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
    use aruna_ops_work::node::counter_check::CounterCheckError;
    use aruna_runner::testing::test_context;
    use aruna_storage::storage::handle::StorageHandle;
    use tempfile::tempdir;

    fn open_storage(dir: &tempfile::TempDir) -> StorageHandle {
        aruna_storage::storage::worker::FjallStorage::open(dir.path().to_str().expect("utf8 path"))
            .expect("storage opens")
    }

    async fn read_usage(storage_handle: &StorageHandle, key: Vec<u8>) -> Option<Vec<u8>> {
        let event = storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: USAGE_STATS_KEYSPACE.to_string(),
                key: key.into(),
                txn_id: None,
            })
            .await;
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            panic!("expected a usage read result, got {event:?}");
        };
        value.map(|value| value.to_vec())
    }

    async fn ensure_usage_counters(driver_ctx: &DriverContext) -> Result<bool, CounterCheckError> {
        drive(CounterCheckOperation::new(), driver_ctx).await
    }

    async fn rebuild(driver_ctx: &DriverContext) {
        use aruna_ops_data::s3::usage_rebuild::RebuildStatsOperation;
        drive(RebuildStatsOperation::new(), driver_ctx)
            .await
            .expect("usage rebuild finishes");
    }

    // Startup only marks the counters incomplete and returns; the rebuild runs later.
    #[tokio::test]
    async fn counters_rebuild_shards() {
        use aruna_core::structs::storage::usage::USAGE_REBUILD_KEY;
        let temp = tempdir().expect("temp dir");
        let storage_handle = open_storage(&temp);
        let driver_ctx = test_context(storage_handle.clone());

        assert!(
            ensure_usage_counters(&driver_ctx).await.unwrap(),
            "missing shards must report a rebuild"
        );
        let marker = read_usage(&storage_handle, USAGE_REBUILD_KEY.to_vec()).await;
        assert!(marker.is_some(), "the counters must count as incomplete");
        let shard = read_usage(&storage_handle, global_shard_keys()[0].clone()).await;
        assert!(shard.is_none(), "startup must not wait for the rebuild");
        assert!(
            ensure_usage_counters(&driver_ctx).await.unwrap(),
            "an unfinished rebuild must be started again"
        );

        rebuild(&driver_ctx).await;
        assert!(
            read_usage(&storage_handle, USAGE_REBUILD_KEY.to_vec())
                .await
                .is_none()
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
    async fn counters_rebuild_buckets() {
        use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
        use aruna_core::structs::storage::blob::BucketInfo;
        use aruna_core::structs::storage::format::Compression;
        use aruna_core::structs::storage::usage::usage_bucket_key;

        let temp = tempdir().expect("temp dir");
        let storage_handle = open_storage(&temp);
        let driver_ctx = test_context(storage_handle.clone());
        assert!(ensure_usage_counters(&driver_ctx).await.unwrap());
        rebuild(&driver_ctx).await;
        // A bucket from before bucket rows existed has shards but no row of its own.
        let info = BucketInfo {
            group_id: ulid::Ulid::from_bytes([3; 16]),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        };
        storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: S3_BUCKET_KEYSPACE.to_string(),
                key: b"older".to_vec().into(),
                value: info.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;

        assert!(
            ensure_usage_counters(&driver_ctx).await.unwrap(),
            "a bucket without a row must report a rebuild"
        );
        rebuild(&driver_ctx).await;
        let event = storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: USAGE_STATS_KEYSPACE.to_string(),
                key: usage_bucket_key("older").into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::ReadResult { value: Some(_), .. })
        ));
        assert!(!ensure_usage_counters(&driver_ctx).await.unwrap());
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
        assert!(acquired.graph_handle.is_none());
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
    // An unreadable stage folder must stop the start, not hide its scratch files from fences.
    #[tokio::test]
    async fn stage_inspection_fails() {
        use aruna_config::settings::read_settings_from;

        let temp = tempdir().expect("temp dir");
        let path = temp.path().to_str().expect("utf8 path").to_string();
        let map: std::collections::BTreeMap<String, String> = [
            ("DATABASE_DIR".to_string(), path),
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
        // A regular file as parent makes the metadata lookup fail with ENOTDIR, even as root.
        let file = temp.path().join("not-a-folder");
        std::fs::write(&file, b"").expect("file written");
        let vfs = VfsSettings {
            stage_dir: Some(file.join("vfs-stage")),
            ..VfsSettings::default()
        };
        let error = open_vfs(&vfs, &config, &storage).expect_err("inspection error propagates");
        assert!(
            error
                .to_string()
                .contains("failed to inspect VFS stage directory")
        );
    }

    #[tokio::test]
    async fn stage_failure_releases() {
        use aruna_config::settings::read_settings_from;

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
                ("DATABASE_DIR".to_string(), path.clone()),
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
            let reopened = aruna_storage::storage::worker::FjallStorage::open(&path);
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
        use aruna_config::settings::read_settings_from;

        let temp = tempdir().expect("temp dir");
        let path = temp.path().to_str().expect("utf8 path").to_string();
        let map: std::collections::BTreeMap<String, String> = [
            ("DATABASE_DIR".to_string(), path.clone()),
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
        let storage = super::open_storage(&settings).expect("storage opens");
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

        let reopened = aruna_storage::storage::worker::FjallStorage::open(&path)
            .expect("cleanup released the store");
        let identity = aruna_config::identity::IdentityStore::from_storage(reopened);
        assert!(
            identity.load().await.unwrap().is_none(),
            "a pre-cancelled startup must not generate or persist an identity"
        );
    }

    /// Settings of a store under `path` that resolve without a network.
    fn reopen_settings(path: &str) -> Settings {
        let map: std::collections::BTreeMap<String, String> = [
            ("DATABASE_DIR", path),
            ("SOCKET_ADDRESS", "127.0.0.1:0"),
            ("S3_HOST", "127.0.0.1:0"),
            ("S3_ADDRESS", "127.0.0.1:0"),
            ("PORTAL_MODE", "disabled"),
            ("ARUNA_FJALL_PERSIST_MODE", "buffer"),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();
        aruna_config::settings::read_settings_from(&map).expect("settings parse")
    }

    // A persisted identity loads unchanged from a reopened store on the repeat boot.
    #[tokio::test]
    async fn boot_reuses_identity() {
        use aruna_config::identity::IdentityStore;
        use aruna_config::identity::enrollment::{EnrollmentPlan, plan_enrollment};
        use aruna_core::structs::identity::auth::NodeCapabilities;

        let dir = tempdir().expect("temp dir");
        let storage = open_storage(&dir);
        let store = IdentityStore::from_storage(storage.clone());
        assert!(store.load().await.unwrap().is_none());
        let generated = store.generate().unwrap();
        store.persist(&generated).await.unwrap();
        let (derived_realm, capabilities) = store.capabilities(&generated).unwrap();
        assert_eq!(derived_realm, generated.realm_id);
        assert!(matches!(capabilities, NodeCapabilities::Management { .. }));
        drop(store);
        storage.close().await;

        let store = IdentityStore::from_storage(open_storage(&dir));
        let loaded = store
            .load()
            .await
            .unwrap()
            .expect("a persisted identity must load on the repeat boot");
        assert_eq!(loaded, generated);
        assert_eq!(
            plan_enrollment(Some(&loaded), false).unwrap(),
            EnrollmentPlan::Ready
        );
    }

    // An incomplete first startup must resume realm initialization after a
    // reopen rather than mint a second identity.
    #[tokio::test]
    async fn incomplete_boot_resumes() {
        use aruna_config::StartupMode;
        use aruna_config::identity::IdentityStore;
        use aruna_core::structs::identity::node_state::PersistedNodeStatus;

        let dir = tempdir().expect("temp dir");
        let storage = open_storage(&dir);
        let store = IdentityStore::from_storage(storage.clone());
        let generated = store.generate().unwrap();
        assert!(matches!(
            generated.status,
            PersistedNodeStatus::PendingInitialization
        ));
        store.persist(&generated).await.unwrap();
        drop(store);
        storage.close().await;

        let path = dir.path().to_str().expect("utf8 path");
        let (config, _storage) = resolve_settings(reopen_settings(path))
            .await
            .expect("settings resolve");
        assert!(matches!(
            config.startup_mode,
            StartupMode::InitializeRealm { .. }
        ));
        assert_eq!(config.node_state, generated);
    }
}
