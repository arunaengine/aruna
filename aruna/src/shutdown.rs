//! Ordered, bounded node shutdown.
//!
//! Ingress and writers stop before blob and storage are closed, drained, and synced.

use std::sync::mpsc::{RecvTimeoutError, SyncSender, sync_channel};
use std::thread;
use std::time::{Duration, Instant};

use aruna_api::error::ServerSetupError;
use aruna_api::monitoring::{MonitoringState, Readiness};
use aruna_blob::blob::BlobHandle;
use aruna_core::shutdown::Shutdown;
use aruna_net::{FORCED_INBOUND_DRAIN, NetHandle, NetShutdownOutcome};
use aruna_operations::jobs::JOB_SHUTDOWN_GRACE;
use aruna_operations::jobs::runtime::JobsRuntime;
use aruna_operations::metadata::MetadataHandle;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

/// Total budget for the ordered sequence. Kubernetes sends SIGKILL after
/// `terminationGracePeriodSeconds` (30s by default), so stay clear of it.
pub const DEFAULT_SHUTDOWN_GRACE: Duration = Duration::from_secs(20);
/// Smallest configured grace that still funds the protected tail: ingress may
/// take a quarter of the budget, leaving the full twelve seconds behind it.
pub const MIN_SHUTDOWN_GRACE: Duration = Duration::from_secs(16);
/// Extra time the watchdog allows before it stops trusting the sequence.
const WATCHDOG_MARGIN: Duration = Duration::from_secs(5);
/// The ingress phase gets at most this fraction of the grace budget, preventing
/// an unfinished response from starving later phases down to hard aborts.
const INGRESS_BUDGET_DIVISOR: u32 = 4;
/// Protected slices for the phases that close the node down. Each is both the
/// most its phase may spend and the floor every earlier phase leaves it; time a
/// phase does not use carries forward to the final sync.
const NET_SLICE: Duration = Duration::from_secs(7);
const METADATA_SLICE: Duration = Duration::from_secs(1);
const BLOB_SLICE: Duration = Duration::from_secs(2);
const STORAGE_SLICE: Duration = Duration::from_secs(2);
/// What the writer phases must leave untouched: every protected slice together.
const TAIL_RESERVE: Duration = Duration::from_secs(
    NET_SLICE.as_secs() + METADATA_SLICE.as_secs() + BLOB_SLICE.as_secs() + STORAGE_SLICE.as_secs(),
);
/// Time reserved for net teardown after the forced inbound join, preventing the
/// outer phase timeout from dropping teardown work.
const NET_TEARDOWN_MARGIN: Duration = Duration::from_secs(2);
/// Time reserved for aborting timer handlers after their drain deadline.
const TASK_ABORT_MARGIN: Duration = Duration::from_secs(2);
/// Exit code when the watchdog has to kill a shutdown that would not finish.
const FORCED_EXIT_CODE: i32 = 75;

pub fn shutdown_grace_env() -> Duration {
    match dotenvy::var("ARUNA_SHUTDOWN_GRACE_SECS") {
        Ok(value) => parse_grace(&value),
        Err(_) => DEFAULT_SHUTDOWN_GRACE,
    }
}

/// A grace below `MIN_SHUTDOWN_GRACE` cannot fund the protected tail, so it is
/// rejected like any other invalid value and the default applies.
fn parse_grace(value: &str) -> Duration {
    match value.trim().parse::<u64>().map(Duration::from_secs) {
        Ok(grace) if grace >= MIN_SHUTDOWN_GRACE => grace,
        _ => {
            warn!(
                value = %value,
                minimum_secs = MIN_SHUTDOWN_GRACE.as_secs(),
                "Ignoring invalid ARUNA_SHUTDOWN_GRACE_SECS; using the default"
            );
            DEFAULT_SHUTDOWN_GRACE
        }
    }
}

/// Kills the process if the graceful sequence itself wedges. Runs on its own OS
/// thread so a blocked runtime worker or a stuck storage thread cannot silence
/// it. Dropping the guard cancels it.
pub struct ForceExitWatchdog {
    _cancel: SyncSender<()>,
}

impl ForceExitWatchdog {
    pub fn arm(deadline: Duration) -> Self {
        let (cancel, cancelled) = sync_channel::<()>(1);
        thread::spawn(move || {
            if let Err(RecvTimeoutError::Timeout) = cancelled.recv_timeout(deadline) {
                eprintln!(
                    "graceful shutdown exceeded its hard deadline of {}s; exiting",
                    deadline.as_secs()
                );
                std::process::exit(FORCED_EXIT_CODE);
            }
        });
        Self { _cancel: cancel }
    }
}

/// What the node has to stop, in the order it has to stop in.
pub struct NodeShutdown {
    pub shutdown: Shutdown,
    pub readiness: Readiness,
    /// `None` once a server has already terminated on its own.
    pub rest: Option<JoinHandle<Result<(), ServerSetupError>>>,
    /// S3 keeps its connection children owned by this handle, so both the
    /// graceful join and the forced abort await their release.
    pub s3: Option<aruna_api::s3::server::S3ServerHandle>,
    /// Portal SPA listener; drains with the other ingress listeners and is
    /// absent when no portal is configured.
    pub portal: Option<JoinHandle<()>>,
    /// Optional session bridge listener; joined with the other ingress tasks.
    pub session_s3: Option<aruna_api::s3::server::S3ServerHandle>,
    /// The monitoring refresher owner. Its sampler is cancelled and awaited
    /// before storage closes; the ops HTTP task above stays separate.
    pub monitoring: Option<Arc<MonitoringState>>,
    pub task_handle: TaskHandle,
    pub jobs_runtime: Arc<JobsRuntime>,
    pub net_handle: Option<NetHandle>,
    pub metadata_handle: Option<MetadataHandle>,
    pub blob_handle: Option<BlobHandle>,
    pub storage_handle: StorageHandle,
    /// Ops listener task; aborted at the very end so `/readyz` and `/healthz`
    /// answer through the whole sequence and the kubelet never SIGKILLs mid-drain.
    pub ops: Option<JoinHandle<()>>,
    pub grace: Duration,
}

/// What the ordered sequence accomplished. A `false` phase flag or a retained
/// persistence failure means an owner or an uncertain commit remains: the
/// process result must not report a clean stop, and no wipe may claim success.
#[derive(Debug, Default)]
pub struct ShutdownOutcome {
    /// Ingress had to be aborted instead of finishing within its slice; every
    /// server and connection child was still released before the sequence
    /// continued.
    pub ingress_forced: bool,
    /// Timer handlers finished without a forced stop or lost acknowledgement.
    pub tasks_drained: bool,
    /// Every job attempt wound down on its own; no lease was handed back.
    pub jobs_drained: bool,
    /// Tracked background children completed before their drain deadline.
    pub background_drained: bool,
    /// The last network attempt's details, when the phase returned a result.
    pub net: Option<NetShutdownOutcome>,
    /// The network phase returned within its slice and joined every child; a
    /// phase dropped by its budget leaves this false without a result.
    pub net_complete: bool,
    /// Metadata persistence flushed successfully.
    pub metadata_flushed: bool,
    /// Blob writes drained before their deadline.
    pub blob_drained: bool,
    /// Accepted storage mutations drained; `false` means they were fenced
    /// before the final sync, so their commit is uncertain.
    pub storage_drained: bool,
    /// The final storage sync succeeded.
    pub storage_synced: bool,
    /// Writes rejected after the storage close.
    pub rejected_writes: u64,
    /// Blob writes rejected after the blob close.
    pub blob_rejected_writes: u64,
}

impl ShutdownOutcome {
    /// True only when every phase released its owners and every persistence
    /// step reported success. This is the admission gate for a wipe.
    pub fn complete(&self) -> bool {
        self.tasks_drained
            && self.jobs_drained
            && self.background_drained
            && self.net_complete
            && self.metadata_flushed
            && self.blob_drained
            && self.storage_drained
            && self.storage_synced
    }
}

impl NodeShutdown {
    pub async fn run(self) -> ShutdownOutcome {
        let started = Instant::now();
        let watchdog = ForceExitWatchdog::arm(self.grace + WATCHDOG_MARGIN);
        let budget = Budget::new(started, self.grace);

        // 1. Stop advertising readiness before anything is torn down.
        self.readiness.begin_drain();
        info!("Shutdown: readiness gate closed, draining");

        // 2. Ingress stops accepting; in-flight requests finish.
        self.shutdown.trigger();
        let ingress = ingress_budget(self.grace, budget.remaining());
        let mut rest = self.rest;
        let mut s3 = self.s3;
        let mut portal = self.portal;
        let mut session_s3 = self.session_s3;
        let mut ingress_complete = false;
        phase("ingress", ingress, async {
            if let Some(rest) = rest.as_mut() {
                let _ = rest.await;
            }
            rest = None;
            if let Some(s3) = s3.as_mut() {
                s3.wait_until_released().await;
            }
            if let Some(portal) = portal.as_mut() {
                let _ = portal.await;
            }
            portal = None;
            if let Some(session_s3) = session_s3.as_mut() {
                session_s3.wait_until_released().await;
            }
            ingress_complete = true;
        })
        .await;
        if !ingress_complete {
            if let Some(rest) = rest.as_ref() {
                rest.abort();
            }
            if let Some(s3) = s3.as_ref() {
                s3.abort();
            }
            if let Some(portal) = portal.as_ref() {
                portal.abort();
            }
            if let Some(session_s3) = session_s3.as_ref() {
                session_s3.abort();
            }
            if let Some(rest) = rest {
                let _ = rest.await;
            }
            // The S3 abort already cancelled its connections; waiting proves
            // every connection child released before the sequence continues.
            if let Some(s3) = s3.as_mut() {
                s3.wait_until_released().await;
            }
            if let Some(portal) = portal {
                let _ = portal.await;
            }
            if let Some(session_s3) = session_s3.as_mut() {
                session_s3.wait_until_released().await;
            }
        }

        // 3. Every writer stops admitting before any of them is waited on: a
        //    phase that runs out of budget must not leave one still accepting.
        self.task_handle.close_admission();
        self.jobs_runtime.close_admission();
        self.shutdown.close_admission();
        if let Some(net_handle) = self.net_handle.as_ref() {
            net_handle.close_admission();
        }
        info!("Shutdown: writer admission closed");

        // 4. Timer handlers drain while the network still works.
        let task_budget = writer_budget(budget.remaining());
        let task_drain = task_drain_budget(task_budget);
        let mut task_report = None;
        phase("tasks", task_budget, async {
            task_report = Some(self.task_handle.shutdown(task_drain).await);
        })
        .await;
        let tasks_drained = task_report.as_ref().is_some_and(|report| report.drained());
        if let Some(report) = task_report {
            if report.drained() {
                info!(
                    in_flight = report.in_flight,
                    "Shutdown: task scheduler drained"
                );
            } else {
                warn!(
                    in_flight = report.in_flight,
                    aborted = report.aborted,
                    scheduler_unavailable = report.scheduler_unavailable,
                    "Shutdown: task scheduler drain incomplete"
                );
            }
        }

        // 5. Job workers write storage: drain them before the close.
        let job_budget = writer_budget(budget.remaining());
        let job_grace = JOB_SHUTDOWN_GRACE.min(job_budget);
        let mut job_report = None;
        phase("jobs", job_budget, async {
            job_report = Some(
                self.jobs_runtime
                    .shutdown(&self.storage_handle, job_grace)
                    .await,
            );
        })
        .await;
        let jobs_drained = job_report
            .as_ref()
            .is_some_and(|report| report.released == 0 && report.skipped == 0);
        if let Some(job_report) = job_report {
            info!(?job_report, "Shutdown: job runtime drained");
        }

        // 6. Stop the queue-lag sampler and await it before storage closes: its
        //    sample task holds the driver context and must not outlive the store.
        //    The ops HTTP task keeps serving; it is aborted only at the end.
        if let Some(monitoring) = self.monitoring.as_ref() {
            monitoring.stop_queue_refresher().await;
        }

        // 6b. Background children write metadata and storage: join them.
        let mut background_drained = false;
        let background_budget = writer_budget(budget.remaining());
        phase("background", background_budget, async {
            background_drained = self.shutdown.drain(background_budget).await;
        })
        .await;
        if !background_drained {
            warn!(
                pending = self.shutdown.tracked_children(),
                "Background children failed to drain before shutdown continued"
            );
        }

        // 7. Network last among the writers: its eviction path re-emits
        //    documents through the inbound handler.
        let mut net_outcome = None;
        let mut net_complete = self.net_handle.is_none();
        if let Some(net_handle) = self.net_handle.as_ref() {
            let phase_budget = phase_slice(
                budget.remaining(),
                METADATA_SLICE + BLOB_SLICE + STORAGE_SLICE,
                NET_SLICE,
            );
            net_complete = false;
            let mut attempt = None;
            phase("net", phase_budget, async {
                attempt = Some(
                    net_handle
                        .shutdown_with_drain(net_drain_budget(phase_budget))
                        .await,
                );
            })
            .await;
            if let Some(attempt) = attempt {
                net_complete = attempt.complete();
                net_outcome = Some(attempt);
            }
            if net_complete {
                net_handle.clear_inbound_handler();
            } else {
                warn!(
                    "Keeping inbound handler registered because network shutdown did not complete"
                );
            }
        }

        // 8. Flush the metadata store.
        let mut metadata_flushed = self.metadata_handle.is_none();
        if let Some(metadata_handle) = self.metadata_handle.as_ref() {
            phase(
                "metadata",
                phase_slice(
                    budget.remaining(),
                    BLOB_SLICE + STORAGE_SLICE,
                    METADATA_SLICE,
                ),
                async {
                    if let Err(error) = metadata_handle.flush_persistence().await {
                        error!(error = %error, "Failed to flush metadata persistence during shutdown");
                    } else {
                        metadata_flushed = true;
                    }
                },
            )
            .await;
        }

        // 9. Close blob writes, then drain the ones registered before the close so
        //    their storage locations land before storage closes.
        let mut blob_drained = self.blob_handle.is_none();
        let blob_rejected = if let Some(blob_handle) = self.blob_handle.as_ref() {
            blob_handle.close_writes();
            let blob_drain = phase_slice(budget.remaining(), STORAGE_SLICE, BLOB_SLICE);
            blob_drained = blob_handle.drain_writes(blob_drain).await;
            if !blob_drained {
                warn!("Blob writes outlived the shutdown drain");
            }
            blob_handle.rejected_writes()
        } else {
            0
        };

        // 10. Close the write path, then drain the mutations accepted before the
        //     close so they commit ahead of the fsync.
        self.storage_handle.close_writes();
        let storage_drain = phase_slice(budget.remaining(), Duration::ZERO, STORAGE_SLICE);
        let storage_drained = self.storage_handle.drain_accepted(storage_drain).await;
        if !storage_drained {
            // Undrained work must not commit behind the final fsync.
            self.storage_handle.fence_mutations();
            warn!("Accepted storage mutations outlived the shutdown drain; mutations fenced");
        }
        let storage_synced = if let Err(error) = self.storage_handle.sync_all().await {
            error!(error = %error, "Failed to sync storage during shutdown");
            false
        } else {
            true
        };
        let rejected = self.storage_handle.rejected_writes();
        if rejected > 0 || blob_rejected > 0 {
            warn!(
                storage_rejected = rejected,
                blob_rejected, "Rejected writes issued after the shutdown close"
            );
        }

        let outcome = ShutdownOutcome {
            ingress_forced: !ingress_complete,
            tasks_drained,
            jobs_drained,
            background_drained,
            net: net_outcome,
            net_complete,
            metadata_flushed,
            blob_drained,
            storage_drained,
            storage_synced,
            rejected_writes: rejected,
            blob_rejected_writes: blob_rejected,
        };
        if outcome.complete() {
            info!(
                elapsed_ms = started.elapsed().as_millis(),
                rejected_writes = rejected,
                blob_rejected_writes = blob_rejected,
                "Shutdown complete"
            );
        } else {
            warn!(
                ?outcome,
                elapsed_ms = started.elapsed().as_millis(),
                "Shutdown incomplete; an owner or persistence step remains unresolved"
            );
        }
        if let Some(ops) = self.ops {
            ops.abort();
            // Awaiting the aborted task proves its resources released instead
            // of detaching behind the final wipe path.
            let _ = ops.await;
        }
        drop(watchdog);
        outcome
    }
}

/// Remaining slice of the total grace budget.
#[derive(Clone, Copy)]
struct Budget {
    started: Instant,
    grace: Duration,
}

impl Budget {
    fn new(started: Instant, grace: Duration) -> Self {
        Self { started, grace }
    }

    fn remaining(&self) -> Duration {
        self.grace.saturating_sub(self.started.elapsed())
    }
}

/// What a protected phase may spend: never into the slices reserved for the
/// phases behind it, and never more than its own slice.
fn phase_slice(remaining: Duration, later_reserves: Duration, slice: Duration) -> Duration {
    remaining.saturating_sub(later_reserves).min(slice)
}

/// What a writer phase may spend: everything except the protected tail. Purely
/// best-effort, since admission is already closed when this reaches zero.
fn writer_budget(remaining: Duration) -> Duration {
    remaining.saturating_sub(TAIL_RESERVE)
}

/// What the ingress phase may spend: its capped fraction of the grace, and
/// never more than the budget that is left.
fn ingress_budget(grace: Duration, remaining: Duration) -> Duration {
    remaining.min(grace / INGRESS_BUDGET_DIVISOR)
}

/// A short phase budget yields zero: skip the soft drain and go straight to the
/// forced teardown.
fn net_drain_budget(phase_budget: Duration) -> Duration {
    phase_budget.saturating_sub(FORCED_INBOUND_DRAIN + NET_TEARDOWN_MARGIN)
}

fn task_drain_budget(phase_budget: Duration) -> Duration {
    phase_budget.saturating_sub(TASK_ABORT_MARGIN)
}

async fn phase<F>(name: &'static str, budget: Duration, future: F)
where
    F: Future<Output = ()>,
{
    let started = Instant::now();
    if tokio::time::timeout(budget, future).await.is_err() {
        warn!(
            phase = name,
            budget_ms = budget.as_millis(),
            "Shutdown phase exceeded its budget; moving on"
        );
        return;
    }
    info!(
        phase = name,
        elapsed_ms = started.elapsed().as_millis(),
        "Shutdown phase complete"
    );
}

/// The conventional fast exit: once the drain is already running, a second
/// SIGTERM or SIGINT means "stop now", not "wait out the grace". Exits with
/// 128 + the signal number; the caller owns the returned future.
pub async fn arm_signal_exit() {
    let Some(code) = second_signal_code().await else {
        return;
    };
    eprintln!("received a second termination signal during shutdown; exiting immediately");
    std::process::exit(code);
}

#[cfg(unix)]
async fn second_signal_code() -> Option<i32> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate()).ok()?;
    let mut interrupt = signal(SignalKind::interrupt()).ok()?;
    tokio::select! {
        _ = terminate.recv() => Some(143),
        _ = interrupt.recv() => Some(130),
    }
}

#[cfg(not(unix))]
async fn second_signal_code() -> Option<i32> {
    tokio::signal::ctrl_c().await.ok().map(|_| 130)
}

/// Installs the handlers now; the returned future resolves on SIGTERM (what
/// Kubernetes sends) or SIGINT. Installing before startup work is the point:
/// a signal with no handler kills the process outright, skipping the drain.
#[cfg(unix)]
pub fn wait_for_signal() -> impl Future<Output = ()> {
    use tokio::signal::unix::{SignalKind, signal};

    let installed = signal(SignalKind::terminate())
        .and_then(|term| signal(SignalKind::interrupt()).map(|interrupt| (term, interrupt)));
    async move {
        let (mut terminate, mut interrupt) = match installed {
            Ok(handlers) => handlers,
            Err(error) => {
                error!(error = %error, "Failed to install termination handlers");
                return;
            }
        };
        let signal_name = tokio::select! {
            _ = terminate.recv() => "SIGTERM",
            _ = interrupt.recv() => "SIGINT",
        };
        info!(signal = signal_name, "Received termination signal");
    }
}

#[cfg(not(unix))]
pub fn wait_for_signal() -> impl Future<Output = ()> {
    async {
        if tokio::signal::ctrl_c().await.is_ok() {
            info!(signal = "CTRL_C", "Received termination signal");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::effects::StorageEffect;
    use aruna_core::errors::StorageError;
    use aruna_core::events::{Event, StorageEvent};
    use std::sync::atomic::{AtomicBool, Ordering};
    use tempfile::tempdir;

    fn node_shutdown(shutdown: Shutdown, storage_handle: StorageHandle) -> NodeShutdown {
        NodeShutdown {
            shutdown,
            readiness: Readiness::new(),
            rest: None,
            s3: None,
            portal: None,
            session_s3: None,
            monitoring: None,
            task_handle: TaskHandle::new(),
            jobs_runtime: JobsRuntime::new(),
            net_handle: None,
            metadata_handle: None,
            blob_handle: None,
            storage_handle,
            ops: None,
            grace: MIN_SHUTDOWN_GRACE,
        }
    }

    fn open_storage(dir: &tempfile::TempDir) -> StorageHandle {
        aruna_storage::FjallStorage::open(dir.path().to_str().expect("utf8 path"))
            .expect("storage opens")
    }

    // A phase whose future never resolves must return without running the rest
    // of it; an ignored budget hangs the test until the job timeout.
    #[tokio::test]
    async fn phase_honors_budget() {
        let mut completed = false;

        phase("stuck", Duration::ZERO, async {
            std::future::pending::<()>().await;
            completed = true;
        })
        .await;

        assert!(!completed);
    }

    // The inbound drain must leave the forced teardown behind it its own time,
    // or the outer phase timeout drops that teardown unrun.
    #[test]
    fn drain_reserves_teardown() {
        assert_eq!(
            net_drain_budget(Duration::from_secs(10)),
            Duration::from_secs(3)
        );
        assert_eq!(net_drain_budget(Duration::from_secs(1)), Duration::ZERO);
    }

    // The task abort command gets time after its drain expires.
    #[test]
    fn task_reserves_abort() {
        assert_eq!(
            task_drain_budget(Duration::from_secs(5)),
            Duration::from_secs(3)
        );
        assert_eq!(task_drain_budget(Duration::from_secs(1)), Duration::ZERO);
    }

    // At the smallest configured grace every protected phase still gets its
    // whole slice, and the writers get what the tail does not claim.
    #[test]
    fn slices_protect_tail() {
        // Worst case at each grace: ingress burned its whole quarter and every
        // phase then spends everything it is allowed.
        for grace in [MIN_SHUTDOWN_GRACE, DEFAULT_SHUTDOWN_GRACE] {
            let mut remaining = grace - grace / INGRESS_BUDGET_DIVISOR;
            let writers = writer_budget(remaining);
            assert_eq!(writers, remaining - TAIL_RESERVE);
            remaining -= writers;
            assert_eq!(remaining, TAIL_RESERVE);

            let net = phase_slice(
                remaining,
                METADATA_SLICE + BLOB_SLICE + STORAGE_SLICE,
                NET_SLICE,
            );
            assert_eq!(net, NET_SLICE);
            remaining -= net;
            let metadata = phase_slice(remaining, BLOB_SLICE + STORAGE_SLICE, METADATA_SLICE);
            assert_eq!(metadata, METADATA_SLICE);
            remaining -= metadata;
            let blob = phase_slice(remaining, STORAGE_SLICE, BLOB_SLICE);
            assert_eq!(blob, BLOB_SLICE);
            remaining -= blob;
            assert_eq!(
                phase_slice(remaining, Duration::ZERO, STORAGE_SLICE),
                STORAGE_SLICE
            );
        }
        // Time an earlier phase did not spend carries forward but never widens
        // a later slice.
        assert_eq!(
            phase_slice(Duration::from_secs(30), Duration::ZERO, STORAGE_SLICE),
            STORAGE_SLICE
        );
    }

    // A programmatically shortened budget saturates instead of underflowing.
    #[test]
    fn slices_saturate_short() {
        assert_eq!(writer_budget(Duration::from_secs(3)), Duration::ZERO);
        assert_eq!(
            phase_slice(Duration::from_secs(3), TAIL_RESERVE, NET_SLICE),
            Duration::ZERO
        );
        assert_eq!(
            phase_slice(Duration::from_millis(500), STORAGE_SLICE, BLOB_SLICE),
            Duration::ZERO
        );
        assert_eq!(
            phase_slice(Duration::from_millis(500), Duration::ZERO, STORAGE_SLICE),
            Duration::from_millis(500)
        );
    }

    // A configured grace too small for the protected tail is invalid, not a
    // shorter sequence.
    #[test]
    fn grace_below_floor() {
        assert_eq!(parse_grace("10"), DEFAULT_SHUTDOWN_GRACE);
        assert_eq!(parse_grace("0"), DEFAULT_SHUTDOWN_GRACE);
        assert_eq!(parse_grace("nonsense"), DEFAULT_SHUTDOWN_GRACE);
        assert_eq!(parse_grace(" 16 "), MIN_SHUTDOWN_GRACE);
        assert_eq!(parse_grace("25"), Duration::from_secs(25));
    }

    #[test]
    fn budget_reaches_zero() {
        let past = Instant::now()
            .checked_sub(Duration::from_millis(200))
            .expect("monotonic clock has room");

        assert_eq!(
            Budget::new(past, Duration::from_millis(100)).remaining(),
            Duration::ZERO
        );
        assert!(Budget::new(Instant::now(), Duration::from_secs(60)).remaining() > Duration::ZERO);
    }

    // The ingress phase never gets more than its fraction of the grace.
    #[test]
    fn ingress_caps_budget() {
        let grace = Duration::from_secs(20);

        assert_eq!(ingress_budget(grace, grace), Duration::from_secs(5));
        assert_eq!(
            ingress_budget(grace, Duration::from_secs(1)),
            Duration::from_secs(1)
        );
    }

    // The whole point of the sequence: after it returns, no write lands.
    #[tokio::test]
    async fn shutdown_closes_storage() {
        let dir = tempdir().expect("temp dir");
        let storage_handle = open_storage(&dir);
        let sequence = node_shutdown(Shutdown::new(), storage_handle.clone());
        let readiness = sequence.readiness.clone();
        readiness.set_ready();

        let outcome = sequence.run().await;

        assert!(outcome.complete());
        assert!(!readiness.is_ready());
        assert!(readiness.is_draining());
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
        assert_eq!(storage_handle.rejected_writes(), 1);
    }

    // Registered children are cancelled and joined before the close.
    #[tokio::test]
    async fn shutdown_joins_children() {
        let dir = tempdir().expect("temp dir");
        let storage_handle = open_storage(&dir);
        let shutdown = Shutdown::new();
        let stopped = Arc::new(AtomicBool::new(false));

        let child_shutdown = shutdown.clone();
        let child_stopped = stopped.clone();
        let child_storage = storage_handle.clone();
        shutdown.spawn(async move {
            child_shutdown.cancelled().await;
            // A child still writing here is inside the drain, so it commits
            // before the close rather than behind it.
            child_storage
                .send_storage_effect(StorageEffect::Write {
                    key_space: "child".to_string(),
                    key: b"key".to_vec().into(),
                    value: b"value".to_vec().into(),
                    txn_id: None,
                })
                .await;
            child_stopped.store(true, Ordering::SeqCst);
        });

        node_shutdown(shutdown.clone(), storage_handle.clone())
            .run()
            .await;

        assert!(stopped.load(Ordering::SeqCst));
        assert_eq!(shutdown.tracked_children(), 0);
        assert_eq!(storage_handle.rejected_writes(), 0);
    }

    // Ingress listeners driven by the shutdown token are joined with the
    // sequence instead of being left to their cancellation token. S3 handles
    // carry their connection tracker and are covered in the listener tests.
    #[tokio::test]
    async fn portal_listener_joined() {
        let dir = tempdir().expect("temp dir");
        let storage_handle = open_storage(&dir);
        let shutdown = Shutdown::new();
        let stopped = Arc::new(AtomicBool::new(false));

        let child_shutdown = shutdown.clone();
        let child_stopped = stopped.clone();
        let handle = tokio::spawn(async move {
            child_shutdown.cancelled().await;
            child_stopped.store(true, Ordering::SeqCst);
        });

        let mut sequence = node_shutdown(shutdown.clone(), storage_handle.clone());
        sequence.portal = Some(handle);

        sequence.run().await;

        assert!(stopped.load(Ordering::SeqCst));
    }

    // An ingress response that never finishes may burn at most its capped slice,
    // not the whole grace.
    #[tokio::test]
    async fn ingress_hang_bounded() {
        let dir = tempdir().expect("temp dir");
        let storage_handle = open_storage(&dir);
        let mut sequence = node_shutdown(Shutdown::new(), storage_handle.clone());
        sequence.rest = Some(tokio::spawn(async {
            std::future::pending::<()>().await;
            Ok(())
        }));
        sequence.grace = Duration::from_secs(2);

        sequence.run().await;

        assert!(storage_handle.writes_closed());
    }

    // With no budget left for any soft drain, every writer must still have
    // stopped admitting work before storage is closed.
    #[tokio::test]
    async fn zero_budget_closes() {
        let dir = tempdir().expect("temp dir");
        let storage_handle = open_storage(&dir);
        let shutdown = Shutdown::new();
        let mut sequence = node_shutdown(shutdown.clone(), storage_handle.clone());
        let task_handle = sequence.task_handle.clone();
        let jobs_runtime = sequence.jobs_runtime.clone();
        sequence.grace = Duration::ZERO;
        assert!(jobs_runtime.available_slots() > 0);

        sequence.run().await;

        assert_eq!(jobs_runtime.available_slots(), 0);
        task_handle.close_admission();
        assert!(shutdown.is_triggered());
        let started = Arc::new(AtomicBool::new(false));
        let child_started = started.clone();
        shutdown.spawn(async move {
            child_started.store(true, Ordering::SeqCst);
        });
        assert_eq!(shutdown.tracked_children(), 0);
        tokio::task::yield_now().await;
        assert!(!started.load(Ordering::SeqCst));
        assert!(storage_handle.writes_closed());
    }

    // A child that ignores cancellation must not hold up the sequence.
    #[tokio::test]
    async fn stuck_child_bounded() {
        let dir = tempdir().expect("temp dir");
        let storage_handle = open_storage(&dir);
        let shutdown = Shutdown::new();
        shutdown.spawn(std::future::pending());
        let mut sequence = node_shutdown(shutdown.clone(), storage_handle.clone());
        sequence.grace = Duration::from_millis(200);

        let outcome = sequence.run().await;

        assert!(shutdown.is_triggered());
        assert!(storage_handle.writes_closed());
        assert!(!outcome.background_drained);
        assert!(!outcome.complete());
    }

    async fn bound_s3_server(
        storage_handle: StorageHandle,
        shutdown: &Shutdown,
    ) -> (std::net::SocketAddr, aruna_api::s3::server::S3ServerHandle) {
        use aruna_api::cors::CorsConfig;
        use aruna_api::s3::server::{S3Server, S3ServerTimeouts};
        use aruna_core::metrics::NodeMetrics;
        use aruna_core::structs::execution::job::RoCrateLimits;
        use aruna_core::structs::identity::realm::RealmId;
        use aruna_operations::driver::DriverContext;
        use std::sync::Arc;

        let driver_ctx = Arc::new(DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let secret = iroh::SecretKey::from_bytes(&[0x58; 32]);
        let server = S3Server::new(
            "127.0.0.1:0",
            "localhost",
            driver_ctx,
            RealmId::from_bytes([0x58; 32]),
            secret.public(),
            aruna_core::credential_encryption::CredentialEncryptionKey::derive(&secret.to_bytes()),
            RoCrateLimits::default(),
            CorsConfig::default(),
            Arc::new(NodeMetrics::new()),
        )
        .await
        .expect("s3 server builds")
        .with_timeouts(S3ServerTimeouts::default());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("s3 listener binds");
        let address = listener.local_addr().expect("s3 address");
        let (_bound, handle) = server
            .run_with_listener(listener, shutdown.token())
            .expect("s3 server runs");
        (address, handle)
    }

    // The real ingress deadline must not lose either S3 owner: both listeners
    // drain active connection work past their slice, so only the retained
    // forced cleanup can release them before storage closes.
    #[tokio::test(start_paused = true)]
    async fn ingress_timeout_retains() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let dir = tempdir().expect("temp dir");
        let storage_handle = open_storage(&dir);
        let shutdown = Shutdown::new();
        let (main_addr, main_s3) = bound_s3_server(storage_handle.clone(), &shutdown).await;
        let (session_addr, session_s3) = bound_s3_server(storage_handle.clone(), &shutdown).await;

        // A request with an unfinished body keeps one connection child busy per
        // listener, so the graceful wait cannot finish inside its slice.
        let mut main_client = tokio::net::TcpStream::connect(main_addr)
            .await
            .expect("main S3 connect");
        main_client
            .write_all(
                b"PUT /bucket/key HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\n\r\nab",
            )
            .await
            .expect("main partial body");
        let mut session_client = tokio::net::TcpStream::connect(session_addr)
            .await
            .expect("session S3 connect");
        session_client
            .write_all(
                b"PUT /bucket/key HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\n\r\nab",
            )
            .await
            .expect("session partial body");
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut sequence = node_shutdown(shutdown.clone(), storage_handle.clone());
        sequence.s3 = Some(main_s3);
        sequence.session_s3 = Some(session_s3);
        // The ingress slice is a quarter of this grace: the busy connections
        // outlive it and only the forced cleanup can stop them.
        sequence.grace = Duration::from_millis(200);
        let started = tokio::time::Instant::now();

        sequence.run().await;

        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(50) && elapsed < Duration::from_secs(5),
            "the ingress phase must expire on its slice, not on the connection, took {elapsed:?}"
        );
        assert!(storage_handle.writes_closed());
        for (label, client) in [("main", &mut main_client), ("session", &mut session_client)] {
            let mut buf = [0u8; 1];
            let released =
                tokio::time::timeout(Duration::from_secs(1), client.read(&mut buf)).await;
            assert!(
                matches!(released, Ok(Ok(0)) | Ok(Err(_))),
                "{label} S3 connection must be released before shutdown returns, got {released:?}"
            );
        }
    }

    // A handler that signals its start and then never completes on its own, so
    // only a forced abort can end the run.
    struct PendingTaskHandler {
        started: Arc<tokio::sync::Notify>,
    }

    impl aruna_tasks::InboundTaskHandler for PendingTaskHandler {
        #[allow(clippy::type_complexity)]
        fn handle_timer<'life0, 'async_trait>(
            &'life0 self,
            _key: aruna_core::task::TaskKey,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            Self: Sync + 'async_trait,
        {
            let started = self.started.clone();
            Box::pin(async move {
                started.notify_one();
                std::future::pending::<()>().await
            })
        }
    }

    // The tasks phase is timeout-wrapped. When its budget expires, the drain
    // future is dropped and the scheduler must still own the running handler,
    // so a resumed drain sees it instead of reporting a clean shutdown.
    #[tokio::test]
    async fn timeout_keeps_handler() {
        use aruna_core::effects::Effect;
        use aruna_core::handle::Handle;
        use aruna_core::task::{TaskEffect, TaskKey};

        let dir = tempdir().expect("temp dir");
        let storage_handle = open_storage(&dir);
        let sequence = node_shutdown(Shutdown::new(), storage_handle);
        let task_handle = sequence.task_handle.clone();
        let started = Arc::new(tokio::sync::Notify::new());
        task_handle
            .set_inbound_handler(Arc::new(PendingTaskHandler {
                started: started.clone(),
            }))
            .await;
        let _ = task_handle
            .send_effect(Effect::Task(TaskEffect::ResetTimer {
                key: TaskKey::DrainSyncOutbox,
                after: Duration::ZERO,
            }))
            .await;
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("handler should start");

        let mut task_report = None;
        phase("tasks", Duration::from_millis(20), async {
            task_report = Some(task_handle.shutdown(Duration::from_secs(30)).await);
        })
        .await;
        assert!(
            task_report.is_none(),
            "the phase budget must interrupt the task drain"
        );

        let resumed = task_handle.shutdown(Duration::ZERO).await;
        assert_eq!(
            resumed.in_flight, 1,
            "the interrupted drain must keep the handler owned"
        );
        assert_eq!(resumed.aborted, 1);
        assert!(!resumed.drained());

        let settled = task_handle.shutdown(Duration::from_secs(1)).await;
        assert!(
            settled.drained(),
            "the forced stop must release the handler"
        );
    }
}
