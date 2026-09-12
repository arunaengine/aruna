//! Background work started after ingress is bound.

use std::sync::Arc;

use aruna_api::monitoring::Readiness;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::shutdown::Shutdown;
use aruna_operations::driver::DriverContext;
use aruna_operations::jobs::drain::restore_drain_timer;
use aruna_operations::jobs::lifecycle::restore_lifecycle_timers;
use aruna_operations::jobs::runtime::JobsRuntime;
use aruna_operations::metadata::spawn_metadata_warmup;
use aruna_operations::node::startup::{RecoveryConfig, RecoveryStatus};
use aruna_operations::s3::session::spawn_session_sweep;
use aruna_operations::tasks::incoming::TaskQueues;
use aruna_tasks::TaskHandle;
use tracing::warn;

use crate::startup::realm::CoreAnnouncement;
use crate::startup::test_hooks;

pub(crate) struct Background {
    pub(crate) realm_id: aruna_core::structs::RealmId,
    pub(crate) node_id: iroh::PublicKey,
    pub(crate) is_initial_boot: bool,
    pub(crate) driver_ctx: Arc<DriverContext>,
    pub(crate) shutdown: Shutdown,
    pub(crate) readiness: Readiness,
    pub(crate) recovery: RecoveryStatus,
    pub(crate) jobs_runtime: Arc<JobsRuntime>,
    pub(crate) task_handle: TaskHandle,
    pub(crate) task_queues: TaskQueues,
    pub(crate) usage_counters_rebuilt: bool,
    pub(crate) core_announcement: CoreAnnouncement,
}

pub(crate) async fn start(background: Background) {
    let Background {
        realm_id,
        node_id,
        is_initial_boot,
        driver_ctx,
        shutdown,
        readiness,
        recovery,
        jobs_runtime,
        task_handle,
        task_queues,
        usage_counters_rebuilt,
        core_announcement,
    } = background;
    // Both listeners are bound and the local safety gate is satisfied.
    readiness.set_ready();

    let core_ctx = driver_ctx.clone();
    let core_cancelled = shutdown.token();
    let CoreAnnouncement {
        documents: core_documents,
        allow_genesis: allow_core_genesis,
    } = core_announcement;
    let core_publish = publish_core(
        core_ctx,
        node_id,
        realm_id,
        allow_core_genesis,
        core_documents,
    );
    shutdown.spawn(async move {
        tokio::select! {
            result = core_publish => {
                if let Err(error) = result {
                    warn!(error = ?error, "Failed to queue core document replication");
                }
            }
            _ = core_cancelled.cancelled() => {}
        }
    });

    // Durable background queues run after admission opens.
    if let Err(error) = jobs_runtime
        .recover_stale_jobs(&driver_ctx.storage_handle)
        .await
    {
        warn!(error = %error, "Failed to recover stale jobs at startup");
    }
    jobs_runtime.start();
    task_queues.start(&shutdown).await;
    restore_drain_timer(&driver_ctx.storage_handle, &task_handle).await;
    restore_lifecycle_timers(&driver_ctx.storage_handle, &task_handle).await;
    spawn_metadata_warmup(driver_ctx.clone(), &shutdown);
    spawn_session_sweep(driver_ctx.clone(), &shutdown);

    let recovery_ctx = driver_ctx.clone();
    let recovery_config = RecoveryConfig {
        realm_id,
        node_id,
        // An unchanged restart republishes nothing: accepted outbox work and
        // document sync history already carry convergence.
        publish_full_usage: usage_counters_rebuilt || is_initial_boot,
    };
    let cancelled = shutdown.token();
    shutdown.spawn(async move {
        test_hooks::recover_child(recovery_ctx, recovery_config, recovery, cancelled).await;
    });
}

async fn publish_core(
    core_ctx: Arc<DriverContext>,
    node_id: iroh::PublicKey,
    realm_id: aruna_core::structs::RealmId,
    allow_genesis: bool,
    documents: Vec<DocumentSyncTarget>,
) -> Result<(), Box<dyn std::error::Error>> {
    // A device announces nothing: it holds no sync topic and is refused one.
    if documents.is_empty() {
        return Ok(());
    }
    test_hooks::core_publication_barrier().await;
    crate::bootstrap::publish_core_documents(
        core_ctx.as_ref(),
        node_id,
        realm_id,
        allow_genesis,
        documents,
    )
    .await
}
