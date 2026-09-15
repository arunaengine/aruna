//! Background work started after ingress is bound.

use std::sync::Arc;

use aruna_api::monitoring::Readiness;
use aruna_core::document::DocumentTarget;
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

use crate::application::{Service, ServiceExit};
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

/// The order the node starts its durable background work. Readiness requires
/// both listeners bound and the safety gate; stale-job recovery precedes job
/// admission, and queues precede their restored timers. A change is behavior.
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

/// What the ordered background start did, in the same required-versus-optional
/// service terms as steady-state supervision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BackgroundOutcome {
    /// Readiness was announced and the phase sequence completed. An optional
    /// listener exit is reported without abandoning later phases.
    Started,
    /// A stop was accepted before the phase sequence completed; no later
    /// phase was admitted.
    Cancelled,
    /// A listener whose loss stops the node exited; the sequence stopped.
    RequiredListenerFailed(Service),
}

/// Runs the background start, applying steady-state supervision to any
/// finished ingress listener between phases: an optional exit is reported and
/// required phases continue; a required exit stops the sequence and returns.
pub(crate) async fn start(
    background: Background,
    stop: &tokio_util::sync::CancellationToken,
    observed_exit: impl Fn() -> Option<(Service, ServiceExit)>,
    mut on_phase: impl FnMut(StartupPhase),
) -> BackgroundOutcome {
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
    let mut completed_all_phases = true;

    for phase in STARTUP_PHASES {
        // The supervision policy applies here exactly as it does in steady
        // state: a required listener stops the sequence, an optional one is
        // reported and the required phases continue.
        match observed_exit() {
            Some((service, ServiceExit::StopsNode)) => {
                return BackgroundOutcome::RequiredListenerFailed(service);
            }
            Some((service, ServiceExit::ReportedOnly)) => {
                warn!(
                    service = ?service,
                    "Optional listener exited during background startup; continuing required phases"
                );
            }
            None => {}
        }
        // A stop accepted while an earlier phase ran must not admit more work.
        if stop.is_cancelled() {
            completed_all_phases = false;
            break;
        }
        on_phase(*phase);
        match phase {
            StartupPhase::Ready => {
                readiness.set_ready();
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
                    .recover_until_stopped(&driver_ctx.storage_handle, stop, || {
                        matches!(observed_exit(), Some((_, ServiceExit::StopsNode)))
                    })
                    .await
                {
                    warn!(error = %error, "Failed to recover stale jobs at startup");
                }
            }
            StartupPhase::StartJobRuntime => jobs_runtime.start(),
            StartupPhase::StartTaskQueues => {
                let task_queues = task_queues.take().expect("task queues start exactly once");
                task_queues
                    .start_until_stopped(&shutdown, stop, || {
                        matches!(observed_exit(), Some((_, ServiceExit::StopsNode)))
                    })
                    .await;
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
    if completed_all_phases {
        BackgroundOutcome::Started
    } else {
        BackgroundOutcome::Cancelled
    }
}

async fn publish_core(
    core_ctx: Arc<DriverContext>,
    node_id: iroh::PublicKey,
    realm_id: aruna_core::structs::RealmId,
    allow_genesis: bool,
    documents: Vec<DocumentTarget>,
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
    async fn stop_before_start() {
        let (background, _temp) = test_background().await;
        let readiness = background.readiness.clone();
        let stop = tokio_util::sync::CancellationToken::new();
        stop.cancel();
        let mut executed = Vec::new();

        let outcome = start(background, &stop, || None, |phase| executed.push(phase)).await;

        assert_eq!(outcome, BackgroundOutcome::Cancelled);
        assert!(executed.is_empty());
        assert!(!readiness.is_ready());
    }

    // A required listener that already exited is observed before the first
    // phase, so the sequence stops and readiness is never announced behind it.
    #[tokio::test]
    async fn required_failure_stops() {
        let (background, _temp) = test_background().await;
        let readiness = background.readiness.clone();
        let mut executed = Vec::new();

        let outcome = start(
            background,
            &tokio_util::sync::CancellationToken::new(),
            || Some((Service::Rest, ServiceExit::StopsNode)),
            |phase| executed.push(phase),
        )
        .await;

        assert_eq!(
            outcome,
            BackgroundOutcome::RequiredListenerFailed(Service::Rest)
        );
        assert!(executed.is_empty());
        assert!(!readiness.is_ready());
    }

    // An optional listener exit before readiness is reported, not fatal: every
    // required phase still runs and readiness is announced.
    #[tokio::test]
    async fn optional_failure_continues() {
        let (background, _temp) = test_background().await;
        let readiness = background.readiness.clone();
        let mut executed = Vec::new();

        let outcome = start(
            background,
            &tokio_util::sync::CancellationToken::new(),
            || Some((Service::SessionS3, ServiceExit::ReportedOnly)),
            |phase| executed.push(phase),
        )
        .await;

        assert_eq!(outcome, BackgroundOutcome::Started);
        assert_eq!(executed, STARTUP_PHASES.to_vec());
        assert!(readiness.is_ready());
    }

    // An optional listener exit between recovery phases must not abandon the
    // phases after it: the sequence still reaches the end and readiness holds.
    #[tokio::test]
    async fn midphase_failure_continues() {
        let (background, _temp) = test_background().await;
        let readiness = background.readiness.clone();
        let mut executed = Vec::new();
        let optional_lost = std::cell::Cell::new(false);
        let observe = |phase: StartupPhase| {
            if phase == StartupPhase::RecoverStaleJobs {
                optional_lost.set(true);
            }
        };

        let outcome = start(
            background,
            &tokio_util::sync::CancellationToken::new(),
            || {
                optional_lost
                    .get()
                    .then_some((Service::SessionS3, ServiceExit::ReportedOnly))
            },
            |phase| {
                observe(phase);
                executed.push(phase);
            },
        )
        .await;

        assert_eq!(outcome, BackgroundOutcome::Started);
        assert_eq!(executed, STARTUP_PHASES.to_vec());
        assert!(readiness.is_ready());
    }

    // A stop accepted between phases stops the sequence, and a stop racing a
    // required failure reports the failure; optional exits never mask a stop.
    #[tokio::test]
    async fn stop_between_phases() {
        let (background, _temp) = test_background().await;
        let stop = tokio_util::sync::CancellationToken::new();
        let mut executed = Vec::new();

        let outcome = start(
            background,
            &stop,
            || None,
            |phase| {
                if phase == StartupPhase::RecoverStaleJobs {
                    stop.cancel();
                }
                executed.push(phase);
            },
        )
        .await;

        assert_eq!(outcome, BackgroundOutcome::Cancelled);
        assert_eq!(
            executed,
            vec![
                StartupPhase::Ready,
                StartupPhase::CorePublication,
                StartupPhase::RecoverStaleJobs
            ]
        );
    }

    // A required listener failure wins over a simultaneous stop: the caller
    // reports the failed service, and no later phase is admitted.
    #[tokio::test]
    async fn required_failure_wins() {
        let (background, _temp) = test_background().await;
        let stop = tokio_util::sync::CancellationToken::new();
        stop.cancel();
        let mut executed = Vec::new();

        let outcome = start(
            background,
            &stop,
            || Some((Service::Rest, ServiceExit::StopsNode)),
            |phase| executed.push(phase),
        )
        .await;

        assert_eq!(
            outcome,
            BackgroundOutcome::RequiredListenerFailed(Service::Rest)
        );
        assert!(executed.is_empty());
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
    fn phases_ordered() {
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
    fn phases_scheduled_once() {
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
