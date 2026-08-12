//! Ordered, bounded node shutdown.
//!
//! Ingress and writers stop before blob and storage are sealed, drained, and synced.

use std::sync::mpsc::{RecvTimeoutError, SyncSender, sync_channel};
use std::thread;
use std::time::{Duration, Instant};

use aruna_api::error::ServerSetupError;
use aruna_api::ops::Readiness;
use aruna_blob::blob::BlobHandle;
use aruna_core::shutdown::Shutdown;
use aruna_net::{FORCED_INBOUND_DRAIN, NetHandle};
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
    pub s3: Option<JoinHandle<()>>,
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

impl NodeShutdown {
    pub async fn run(self) {
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
        let mut ingress_complete = false;
        phase("ingress", ingress, async {
            if let Some(rest) = rest.as_mut() {
                let _ = rest.await;
            }
            rest = None;
            if let Some(s3) = s3.as_mut() {
                let _ = s3.await;
            }
            s3 = None;
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
            if let Some(rest) = rest {
                let _ = rest.await;
            }
            if let Some(s3) = s3 {
                let _ = s3.await;
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
        if let Some(report) = task_report {
            info!(
                in_flight = report.in_flight,
                aborted = report.aborted,
                "Shutdown: task scheduler drained"
            );
        }

        // 5. Job workers write storage: drain them before the seal.
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
        if let Some(job_report) = job_report {
            info!(?job_report, "Shutdown: job runtime drained");
        }

        // 6. Background children write metadata and storage: join them.
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
        if let Some(net_handle) = self.net_handle.as_ref() {
            let phase_budget = phase_slice(
                budget.remaining(),
                METADATA_SLICE + BLOB_SLICE + STORAGE_SLICE,
                NET_SLICE,
            );
            let mut net_shutdown_complete = false;
            phase("net", phase_budget, async {
                net_shutdown_complete = net_handle
                    .shutdown_with_drain(net_drain_budget(phase_budget))
                    .await;
            })
            .await;
            if net_shutdown_complete {
                net_handle.clear_inbound_handler();
            } else {
                warn!(
                    "Keeping inbound handler registered because network shutdown did not complete"
                );
            }
        }

        // 8. Flush the metadata store.
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
                    }
                },
            )
            .await;
        }

        // 9. Seal blob writes, then drain the ones registered before the seal so
        //    their storage locations land before storage closes.
        let blob_rejected = if let Some(blob_handle) = self.blob_handle.as_ref() {
            blob_handle.seal();
            let blob_drain = phase_slice(budget.remaining(), STORAGE_SLICE, BLOB_SLICE);
            if !blob_handle.drain_writes(blob_drain).await {
                warn!("Blob writes outlived the shutdown drain");
            }
            blob_handle.rejected_writes()
        } else {
            0
        };

        // 10. Seal the write path, then drain the mutations accepted before the
        //     seal so they commit ahead of the fsync.
        self.storage_handle.seal();
        let storage_drain = phase_slice(budget.remaining(), Duration::ZERO, STORAGE_SLICE);
        if !self.storage_handle.drain_accepted(storage_drain).await {
            // Undrained work must not commit behind the final fsync.
            self.storage_handle.fence_mutations();
            warn!("Accepted storage mutations outlived the shutdown drain; mutations fenced");
        }
        if let Err(error) = self.storage_handle.sync_all().await {
            error!(error = %error, "Failed to sync storage during shutdown");
        }
        let rejected = self.storage_handle.rejected_writes();
        if rejected > 0 || blob_rejected > 0 {
            warn!(
                storage_rejected = rejected,
                blob_rejected, "Rejected writes issued after the shutdown seal"
            );
        }

        info!(
            elapsed_ms = started.elapsed().as_millis(),
            rejected_writes = rejected,
            blob_rejected_writes = blob_rejected,
            "Shutdown complete"
        );
        if let Some(ops) = self.ops {
            ops.abort();
        }
        drop(watchdog);
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

/// Arms the conventional fast exit: once the drain is already running, a
/// second SIGTERM or SIGINT means "stop now", not "wait out the grace". Exits
/// with 128 + the signal number, like a process without handlers would.
pub fn arm_signal_exit() -> JoinHandle<()> {
    tokio::spawn(async {
        let Some(code) = second_signal_code().await else {
            return;
        };
        eprintln!("received a second termination signal during shutdown; exiting immediately");
        std::process::exit(code);
    })
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
    async fn shutdown_seals_storage() {
        let dir = tempdir().expect("temp dir");
        let storage_handle = open_storage(&dir);
        let sequence = node_shutdown(Shutdown::new(), storage_handle.clone());
        let readiness = sequence.readiness.clone();
        readiness.set_ready();

        sequence.run().await;

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
                error: StorageError::Sealed
            })
        ));
        assert_eq!(storage_handle.rejected_writes(), 1);
    }

    // Registered children are cancelled and joined before the seal.
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
            // before the seal rather than behind it.
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

        assert!(storage_handle.is_sealed());
    }

    // With no budget left for any soft drain, every writer must still have
    // stopped admitting work before storage is sealed.
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
        assert!(storage_handle.is_sealed());
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

        sequence.run().await;

        assert!(shutdown.is_triggered());
        assert!(storage_handle.is_sealed());
    }
}
