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

/// One action in the durable background start. `start` runs every phase in
/// `STARTUP_PHASES` order, so the agreed order is the executed order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StartupPhase {
    Ready,
    CorePublication,
    RecoverStaleJobs,
    StartJobRuntime,
    StartTaskQueues,
    RestoreDrainTimer,
    RestoreLifecycleTimers,
    WarmMetadata,
    SweepSessions,
    RecoverChild,
}

/// The order the node starts its durable background work. Readiness is
/// announced only once both listeners are bound and the local safety gate is
/// satisfied; stale-job recovery runs before the job runtime admits work; and
/// durable queues start before their restored timers. A change here is a
/// startup behavior change, not a side effect of refactoring.
pub(crate) const STARTUP_PHASES: &[StartupPhase] = &[
    StartupPhase::Ready,
    StartupPhase::CorePublication,
    StartupPhase::RecoverStaleJobs,
    StartupPhase::StartJobRuntime,
    StartupPhase::StartTaskQueues,
    StartupPhase::RestoreDrainTimer,
    StartupPhase::RestoreLifecycleTimers,
    StartupPhase::WarmMetadata,
    StartupPhase::SweepSessions,
    StartupPhase::RecoverChild,
];

/// Runs the background start, checking for an accepted stop and for an
/// already-finished ingress listener between phases. Returns whether readiness
/// was announced; a `false` result means no phase that depends on it ran.
pub(crate) async fn start(
    background: Background,
    stop: &tokio_util::sync::CancellationToken,
    listener_lost: impl Fn() -> bool,
) -> bool {
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
    let CoreAnnouncement {
        documents,
        allow_genesis: allow_core_genesis,
    } = core_announcement;
    let mut core_documents = Some(documents);
    let mut task_queues = Some(task_queues);
    let mut ready_announced = false;

    for phase in STARTUP_PHASES {
        // A stop accepted while an earlier phase ran must not admit more work,
        // and a listener that already exited must stop the node, not start
        // recovery behind it.
        if stop.is_cancelled() || listener_lost() {
            break;
        }
        match phase {
            StartupPhase::Ready => {
                readiness.set_ready();
                ready_announced = true;
            }
            StartupPhase::CorePublication => {
                let documents = core_documents
                    .take()
                    .expect("core publication runs exactly once");
                let core_ctx = driver_ctx.clone();
                let core_cancelled = shutdown.token();
                let core_publish =
                    publish_core(core_ctx, node_id, realm_id, allow_core_genesis, documents);
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
            }
            StartupPhase::RecoverStaleJobs => {
                if let Err(error) = jobs_runtime
                    .recover_stale_jobs(&driver_ctx.storage_handle)
                    .await
                {
                    warn!(error = %error, "Failed to recover stale jobs at startup");
                }
            }
            StartupPhase::StartJobRuntime => jobs_runtime.start(),
            StartupPhase::StartTaskQueues => {
                let task_queues = task_queues.take().expect("task queues start exactly once");
                task_queues.restore_timers_and_start(&shutdown).await;
            }
            StartupPhase::RestoreDrainTimer => {
                restore_drain_timer(&driver_ctx.storage_handle, &task_handle).await;
            }
            StartupPhase::RestoreLifecycleTimers => {
                restore_lifecycle_timers(&driver_ctx.storage_handle, &task_handle).await;
            }
            StartupPhase::WarmMetadata => spawn_metadata_warmup(driver_ctx.clone(), &shutdown),
            StartupPhase::SweepSessions => spawn_session_sweep(driver_ctx.clone(), &shutdown),
            StartupPhase::RecoverChild => {
                let recovery_config = RecoveryConfig {
                    realm_id,
                    node_id,
                    // An unchanged restart republishes nothing: accepted outbox work and
                    // document sync history already carry convergence.
                    publish_full_usage: usage_counters_rebuilt || is_initial_boot,
                };
                let cancelled = shutdown.token();
                let recovery_ctx = driver_ctx.clone();
                let recovery = recovery.clone();
                shutdown.spawn(async move {
                    test_hooks::recover_child(recovery_ctx, recovery_config, recovery, cancelled)
                        .await;
                });
            }
        }
    }
    ready_announced
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

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_operations::tasks::incoming::install_task_queues;

    async fn test_background() -> (Background, tempfile::TempDir) {
        let temp = tempfile::tempdir().expect("temp dir");
        let storage = aruna_storage::FjallStorage::open(temp.path().to_str().expect("utf8 path"))
            .expect("storage opens");
        let driver_ctx = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let task_handle = TaskHandle::new();
        let jobs_runtime = JobsRuntime::new_paused();
        let task_queues = install_task_queues(
            driver_ctx.clone(),
            task_handle.clone(),
            jobs_runtime.clone(),
            aruna_core::structs::RoCrateLimits::default(),
        )
        .await;
        let background = Background {
            realm_id: aruna_core::structs::RealmId::from_bytes([7u8; 32]),
            node_id: iroh::SecretKey::from_bytes(&[8u8; 32]).public(),
            is_initial_boot: false,
            driver_ctx,
            shutdown: Shutdown::new(),
            readiness: Readiness::new(),
            recovery: RecoveryStatus::new(),
            jobs_runtime,
            task_handle,
            task_queues,
            usage_counters_rebuilt: false,
            core_announcement: CoreAnnouncement {
                documents: Vec::new(),
                allow_genesis: false,
            },
        };
        (background, temp)
    }

    // A stop accepted before the first phase must not announce readiness or
    // start any recovery step.
    #[tokio::test]
    async fn accepted_stop_announces_no_readiness() {
        let (background, _temp) = test_background().await;
        let readiness = background.readiness.clone();
        let stop = tokio_util::sync::CancellationToken::new();
        stop.cancel();

        let announced = start(background, &stop, || false).await;

        assert!(!announced);
        assert!(!readiness.is_ready());
    }

    // An ingress listener that already exited is observed between startup
    // phases, so readiness is never announced behind a dead listener.
    #[tokio::test]
    async fn lost_listener_announces_no_readiness() {
        let (background, _temp) = test_background().await;
        let readiness = background.readiness.clone();

        let announced = start(
            background,
            &tokio_util::sync::CancellationToken::new(),
            || true,
        )
        .await;

        assert!(!announced);
        assert!(!readiness.is_ready());
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;

    const ALL_PHASES: [StartupPhase; 10] = [
        StartupPhase::Ready,
        StartupPhase::CorePublication,
        StartupPhase::RecoverStaleJobs,
        StartupPhase::StartJobRuntime,
        StartupPhase::StartTaskQueues,
        StartupPhase::RestoreDrainTimer,
        StartupPhase::RestoreLifecycleTimers,
        StartupPhase::WarmMetadata,
        StartupPhase::SweepSessions,
        StartupPhase::RecoverChild,
    ];

    // The executed order is the tested order: readiness before recovery, the
    // job runtime opening only after stale recovery, queues before their timers.
    #[test]
    fn startup_phases_run_in_order() {
        let positions: Vec<usize> = STARTUP_PHASES
            .iter()
            .map(|phase| ALL_PHASES.iter().position(|known| known == phase).unwrap())
            .collect();
        let mut sorted = positions.clone();
        sorted.sort_unstable();
        assert_eq!(
            positions, sorted,
            "startup phases must keep their agreed order"
        );
        assert_eq!(STARTUP_PHASES[0], StartupPhase::Ready);
        assert!(
            index_of(StartupPhase::RecoverStaleJobs) < index_of(StartupPhase::StartJobRuntime),
            "stale recovery must run before the job runtime admits work"
        );
        assert!(
            index_of(StartupPhase::StartTaskQueues) < index_of(StartupPhase::RestoreDrainTimer),
            "queues must start before their drain timer is restored"
        );
    }

    // Every phase is scheduled exactly once; a new phase that nobody schedules
    // would otherwise be silently skipped.
    #[test]
    fn every_phase_is_scheduled_once() {
        for phase in ALL_PHASES {
            assert_eq!(
                STARTUP_PHASES
                    .iter()
                    .filter(|scheduled| **scheduled == phase)
                    .count(),
                1,
                "{phase:?} must be scheduled exactly once"
            );
        }
    }

    fn index_of(phase: StartupPhase) -> usize {
        STARTUP_PHASES
            .iter()
            .position(|scheduled| *scheduled == phase)
            .expect("phase is scheduled")
    }
}
