//! Construction of the long-lived node resources.
//!
//! Each resource is acquired in the order the node needs it. `Acquired` owns
//! everything obtained so far, so a failure part-way through runs the ordered
//! teardown for exactly the acquired subset instead of leaking detached tasks.

use std::sync::Arc;

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
use aruna_operations::tasks::incoming::{TaskQueues, initialize_task_holder};
use aruna_tasks::TaskHandle;
use tokio::net::TcpListener;
use tracing::{error, info, warn};

use crate::compute_setup::build_registry;
use crate::config::{Config, load};
use crate::shutdown::{NodeShutdown, shutdown_grace_env};

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
    ops_handle: Option<tokio::task::JoinHandle<()>>,
    task_queues: Option<TaskQueues>,
    usage_counters_rebuilt: bool,
}

impl Acquired {
    fn new(storage_handle: aruna_storage::StorageHandle) -> Self {
        Self {
            storage_handle,
            task_handle: TaskHandle::new(),
            jobs_runtime: JobsRuntime::new_paused(),
            shutdown: Shutdown::new(),
            metrics: Arc::new(NodeMetrics::new()),
            readiness: Readiness::new(),
            recovery: RecoveryStatus::new(),
            net_handle: None,
            metadata_handle: None,
            blob_handle: None,
            driver_ctx: None,
            ops_handle: None,
            task_queues: None,
            usage_counters_rebuilt: false,
        }
    }

    /// Runs the ordered teardown for the acquired subset: ingress (none here),
    /// admissions, tasks, jobs, background children, network, metadata, blob,
    /// then storage, and aborts the ops listener last.
    pub(crate) async fn cleanup(self) {
        info!("Startup stopped early; releasing the acquired resources");
        NodeShutdown {
            shutdown: self.shutdown,
            readiness: self.readiness,
            rest: None,
            s3: None,
            portal: None,
            task_handle: self.task_handle,
            jobs_runtime: self.jobs_runtime,
            net_handle: self.net_handle,
            metadata_handle: self.metadata_handle,
            blob_handle: self.blob_handle,
            storage_handle: self.storage_handle,
            ops: self.ops_handle,
            grace: shutdown_grace_env(),
        }
        .run()
        .await;
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
            ops_handle: self.ops_handle.expect("acquisition starts the ops server"),
        }
    }
}

pub(crate) async fn acquire() -> Result<NodeResources, Box<dyn std::error::Error>> {
    let (config, storage_handle) = load().await?;
    let mut acquired = Acquired::new(storage_handle);

    match fill(&config, &mut acquired).await {
        Ok(()) => Ok(acquired.finish(config)),
        Err(error) => {
            acquired.cleanup().await;
            Err(error)
        }
    }
}

/// Acquires every fallible resource in order, assigning each one to `acquired`
/// as soon as it exists so a later failure owns it.
async fn fill(config: &Config, acquired: &mut Acquired) -> Result<(), Box<dyn std::error::Error>> {
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
        acquired.storage_handle.clone(),
    )
    .await?;
    if let Err(error) = net_handle.reload_realm_peers().await {
        warn!(error = %error, "Failed to refresh realm peers from persisted config during startup");
    }
    acquired.net_handle = Some(net_handle.clone());

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

    let blob_handle = BlobHandler::with_registry(
        BackendRegistry::from_config(&config.blob_backends).map_err(std::io::Error::other)?,
        acquired.storage_handle.clone(),
        net_handle.clone(),
        EgressPolicy::strict().with_deny(config.blob_backends.extra_deny.clone()),
    )
    .await?;
    acquired.blob_handle = Some(blob_handle.clone());

    let compute_handle = build_registry(config)
        .await
        .map_err(std::io::Error::other)?;

    let driver_ctx = Arc::new(DriverContext {
        storage_handle: acquired.storage_handle.clone(),
        net_handle: Some(net_handle),
        blob_handle: Some(blob_handle),
        metadata_handle: Some(metadata_handle),
        task_handle: Some(acquired.task_handle.clone()),
        compute_handle: compute_handle.clone(),
    });

    // Start ops before realm bootstrap so readiness reports startup failure.
    let ops_state = MonitoringState::with_recovery(
        driver_ctx.clone(),
        acquired.metrics.clone(),
        acquired.readiness.clone(),
        acquired.recovery.clone(),
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
    acquired.ops_handle = Some(ops_handle);
    acquired.driver_ctx = Some(driver_ctx.clone());

    // A rebuild is the only local evidence that counters were not carried over.
    acquired.usage_counters_rebuilt = ensure_usage_counters(driver_ctx.as_ref()).await?;

    // Bind compute reconciliation before startup recovery.
    initialize_net_holder(
        driver_ctx.clone(),
        config.rocrate_limits.clone(),
        acquired.jobs_runtime.clone(),
        &acquired.shutdown,
    );
    let task_queues = initialize_task_holder(
        driver_ctx.clone(),
        acquired.task_handle.clone(),
        acquired.jobs_runtime.clone(),
        config.rocrate_limits.clone(),
    )
    .await;
    acquired.task_queues = Some(task_queues);

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

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::effects::StorageEffect;
    use aruna_core::errors::StorageError;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::USAGE_STATS_KEYSPACE;
    use aruna_core::structs::{UsageCounters, global_shard_keys};
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

    // A failure with no fallible resource acquired yet still releases storage,
    // and later acquisitions are absent rather than faked.
    #[tokio::test]
    async fn cleanup_releases_the_acquired_subset() {
        let temp = tempdir().expect("temp dir");
        let storage_handle = open_storage(&temp);
        let acquired = Acquired::new(storage_handle.clone());
        assert!(acquired.net_handle.is_none());
        assert!(acquired.metadata_handle.is_none());
        assert!(acquired.ops_handle.is_none());

        acquired.cleanup().await;

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
}
