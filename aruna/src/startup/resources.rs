//! Construction of the long-lived node resources.
//!
//! Each resource is acquired in the order the node needs it; callers that fail
//! later own the cleanup of what this acquired.

use std::sync::Arc;

use aruna_api::monitoring::{MonitoringState, Readiness, serve_ops};
use aruna_blob::blob::{BackendRegistry, BlobHandler};
use aruna_core::egress::EgressPolicy;
use aruna_core::metrics::NodeMetrics;
use aruna_core::shutdown::Shutdown;
use aruna_net::{NetConfig, NetHandle};
use aruna_operations::driver::DriverContext;
use aruna_operations::jobs::runtime::JobsRuntime;
use aruna_operations::metadata::{MetadataHandle, MetadataHandleOptions};
use aruna_operations::node::startup::RecoveryStatus;
use aruna_operations::sync::incoming::initialize_net_holder;
use aruna_operations::tasks::incoming::{TaskQueues, initialize_task_holder};
use aruna_tasks::TaskHandle;
use tokio::net::TcpListener;
use tracing::{error, info, warn};

use crate::compute_setup::build_registry;
use crate::config::{Config, load};

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
    pub(crate) ops_handle: tokio::task::JoinHandle<()>,
}

pub(crate) async fn acquire() -> Result<NodeResources, Box<dyn std::error::Error>> {
    let (config, storage_handle) = load().await?;
    let net_handle = NetHandle::new(
        NetConfig {
            bind_addr: config.p2p_socket_addr,
            secret_key: Some(config.net_secret_key.clone()),
            realm_id: config.realm_id,
            peer_nodes: config.peer_nodes.clone(),
            peer_endpoints: config.peer_endpoints.clone(),
            temporary_bootstrap_active: config.temporary_bootstrap_active,
            discovery_method: config.discovery_method.clone(),
            relay_method: config.relay_method.clone(),
            max_concurrent_uni_streams: config.max_concurrent_uni_streams,
            max_concurrent_bidi_streams: config.max_concurrent_bidi_streams,
            document_sync_storage_path: Some(config.document_sync_storage_path.clone()),
            document_sync_runtime: Some(config.document_sync_runtime),
            fjall_persist_policy: config.fjall_persist_policy,
        },
        storage_handle.clone(),
    )
    .await?;
    if let Err(error) = net_handle.reload_realm_peers().await {
        warn!(error = %error, "Failed to refresh realm peers from persisted config during startup");
    }
    let task_handle = TaskHandle::new();
    let metadata_handle = MetadataHandle::new_with_options(
        &config.metadata_storage_path,
        config.node_id,
        storage_handle.clone(),
        Some(net_handle.clone()),
        Some(net_handle.document_sync_node()),
        Some(net_handle.document_sync_database()),
        MetadataHandleOptions::default()
            .with_search_storage(config.metadata_search_storage)
            .with_sync_policy(config.fjall_persist_policy),
    )?;
    let blob_handle = BlobHandler::with_registry(
        BackendRegistry::from_config(&config.blob_backends).map_err(std::io::Error::other)?,
        storage_handle.clone(),
        net_handle.clone(),
        EgressPolicy::strict().with_deny(config.blob_backends.extra_deny.clone()),
    )
    .await?;

    let compute_handle = build_registry(&config)
        .await
        .map_err(std::io::Error::other)?;

    let driver_ctx = Arc::new(DriverContext {
        storage_handle,
        net_handle: Some(net_handle.clone()),
        blob_handle: Some(blob_handle),
        metadata_handle: Some(metadata_handle),
        task_handle: Some(task_handle.clone()),
        compute_handle: compute_handle.clone(),
    });

    // One cancellation path for the whole node: background children register
    // here so an ordered shutdown can drain them before storage is closed.
    let shutdown = Shutdown::new();

    // Start ops before realm bootstrap so readiness reports startup failure.
    let metrics = Arc::new(NodeMetrics::new());
    let readiness = Readiness::new();
    let recovery = RecoveryStatus::new();
    let ops_state = MonitoringState::with_recovery(
        driver_ctx.clone(),
        metrics.clone(),
        readiness.clone(),
        recovery.clone(),
    )
    .await;
    let ops_listener = TcpListener::bind(config.ops_socket_addr).await?;
    let bound = ops_listener.local_addr()?;
    let ops_handle = tokio::spawn(async move {
        if let Err(error) = serve_ops(ops_listener, ops_state).await {
            error!(error = %error, "Ops server stopped");
        }
    });
    info!(ops_address = %bound, "Ops server listening");

    // A rebuild is the only local evidence that counters were not carried over.
    let usage_counters_rebuilt = ensure_usage_counters(driver_ctx.as_ref()).await?;

    // Bind compute reconciliation before startup recovery.
    let jobs_runtime = JobsRuntime::new_paused();
    initialize_net_holder(
        driver_ctx.clone(),
        config.rocrate_limits.clone(),
        jobs_runtime.clone(),
        &shutdown,
    );
    let task_queues = initialize_task_holder(
        driver_ctx.clone(),
        task_handle.clone(),
        jobs_runtime.clone(),
        config.rocrate_limits.clone(),
    )
    .await;

    Ok(NodeResources {
        config,
        driver_ctx,
        net_handle,
        shutdown,
        metrics,
        readiness,
        recovery,
        jobs_runtime,
        task_handle,
        task_queues,
        usage_counters_rebuilt,
        ops_handle,
    })
}

/// Ensures the maintained usage counter shards exist before background writes
/// start, and reports whether that required a rebuild.
async fn ensure_usage_counters(
    driver_ctx: &DriverContext,
) -> Result<bool, Box<dyn std::error::Error>> {
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::USAGE_STATS_KEYSPACE;
    use aruna_core::structs::global_shard_keys;
    use aruna_operations::driver::drive;
    use aruna_operations::node::usage_stats::RebuildUsageStatsOperation;

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
                drive(RebuildUsageStatsOperation::new(), driver_ctx).await?;
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
