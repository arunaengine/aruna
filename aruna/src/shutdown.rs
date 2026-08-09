//! Ordered node shutdown.
//!
//! One cancellation path (SIGTERM, SIGINT, or a subsystem failure) drives a
//! fixed sequence of phases. Each phase is bounded, the whole sequence is
//! bounded, and a watchdog thread force-exits the process if even that hangs.
//!
//! The order is dictated by what writes to what:
//!
//! 1. Readiness flips to draining: `/readyz` on the ops listener starts failing,
//!    so Kubernetes stops routing new requests here before anything is torn
//!    down. The ops listener keeps answering for the whole sequence; it is only
//!    aborted after the final storage sync so the kubelet does not SIGKILL us
//!    mid-drain.
//! 2. Ingress (REST, S3) stops accepting and finishes in-flight requests.
//! 3. Timer handlers drain while the network is still up.
//! 4. Job workers write storage: they drain before the seal.
//! 5. Background children (reconcile, gauges, maintenance) are cancelled and
//!    joined; they write metadata and storage.
//! 6. The network shuts down. Its eviction path re-emits documents through the
//!    still-registered inbound handler, so the handler is only cleared after.
//! 7. The metadata store is flushed.
//! 8. Blob writes are sealed and drained, so a mutation's storage location
//!    lands before storage is sealed.
//! 9. Storage is sealed, the mutations accepted before the seal are drained,
//!    then storage is synced. Nothing can commit after the seal.

use std::sync::mpsc::{RecvTimeoutError, SyncSender, sync_channel};
use std::thread;
use std::time::{Duration, Instant};

use aruna_api::error::ServerSetupError;
use aruna_api::ops::Readiness;
use aruna_blob::blob::BlobHandle;
use aruna_core::shutdown::Shutdown;
use aruna_net::NetHandle;
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
/// Extra time the watchdog allows before it stops trusting the sequence.
const WATCHDOG_MARGIN: Duration = Duration::from_secs(5);
/// The ingress phase gets at most this fraction of the grace budget. It waits
/// for every in-flight response, so a response that never finishes (a client
/// that keeps a stream open, a stalled upload) must not starve the later
/// phases down to hard aborts.
const INGRESS_BUDGET_DIVISOR: u32 = 4;
/// Minimum time every draining phase leaves for the phases behind it, so a
/// stuck child cannot consume the budget the network, metadata, blob and
/// storage phases need to close cleanly.
const TAIL_BUDGET_RESERVE: Duration = Duration::from_secs(5);
/// Slice of the net phase reserved for the teardown behind the inbound drain.
/// The drain deadline must sit strictly below the phase budget: with both
/// equal, a drain that times out consumes the whole phase and the outer timeout
/// drops the teardown unrun.
const NET_TEARDOWN_HEADROOM: Duration = Duration::from_secs(2);
/// Exit code when the watchdog has to kill a shutdown that would not finish.
const FORCED_EXIT_CODE: i32 = 75;

pub fn shutdown_grace_from_env() -> Duration {
    match dotenvy::var("ARUNA_SHUTDOWN_GRACE_SECS") {
        Ok(value) => match value.trim().parse::<u64>() {
            Ok(secs) if secs > 0 => Duration::from_secs(secs),
            _ => {
                warn!(
                    value = %value,
                    "Ignoring invalid ARUNA_SHUTDOWN_GRACE_SECS; using the default"
                );
                DEFAULT_SHUTDOWN_GRACE
            }
        },
        Err(_) => DEFAULT_SHUTDOWN_GRACE,
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
        let ingress_budget = budget.remaining().min(self.grace / INGRESS_BUDGET_DIVISOR);
        let mut rest = self.rest;
        let mut s3 = self.s3;
        let mut ingress_complete = false;
        phase("ingress", ingress_budget, async {
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

        // 3. Timer handlers drain while the network still works.
        let report = self
            .task_handle
            .shutdown(tail_reserved(budget.remaining()))
            .await;
        info!(
            in_flight = report.in_flight,
            aborted = report.aborted,
            "Shutdown: task scheduler drained"
        );

        // 4. Job workers write storage: drain them before the seal.
        let job_grace = JOB_SHUTDOWN_GRACE.min(tail_reserved(budget.remaining()));
        let job_report = self
            .jobs_runtime
            .shutdown(&self.storage_handle, job_grace)
            .await;
        info!(?job_report, "Shutdown: job runtime drained");

        // 5. Background children write metadata and storage: join them.
        let mut background_drained = false;
        let background_budget = tail_reserved(budget.remaining());
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

        // 6. Network last among the writers: its eviction path re-emits
        //    documents through the inbound handler.
        if let Some(net_handle) = self.net_handle.as_ref() {
            let phase_budget = budget.remaining();
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

        // 7. Flush the metadata store.
        if let Some(metadata_handle) = self.metadata_handle.as_ref() {
            phase("metadata", budget.remaining(), async {
                if let Err(error) = metadata_handle.flush_persistence().await {
                    error!(error = %error, "Failed to flush metadata persistence during shutdown");
                }
            })
            .await;
        }

        // 8. Seal blob writes, then drain the ones registered before the seal so
        //    their storage locations land before storage closes.
        let blob_rejected = if let Some(blob_handle) = self.blob_handle.as_ref() {
            blob_handle.seal();
            let blob_drain = budget.remaining().min(TAIL_BUDGET_RESERVE);
            if !blob_handle.drain_writes(blob_drain).await {
                warn!("Blob writes outlived the shutdown drain");
            }
            blob_handle.rejected_writes()
        } else {
            0
        };

        // 9. Seal the write path, then drain the mutations accepted before the
        //    seal so they commit ahead of the fsync.
        self.storage_handle.seal();
        let storage_drain = budget.remaining().min(TAIL_BUDGET_RESERVE);
        if !self.storage_handle.drain_accepted(storage_drain).await {
            warn!("Accepted storage mutations outlived the shutdown drain");
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

/// What a draining phase may spend: the remaining budget minus the floor the
/// phases behind it need.
fn tail_reserved(remaining: Duration) -> Duration {
    remaining.saturating_sub(TAIL_BUDGET_RESERVE)
}

fn net_drain_budget(phase_budget: Duration) -> Duration {
    phase_budget.saturating_sub(NET_TEARDOWN_HEADROOM)
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
pub fn escalate_on_second_signal() -> JoinHandle<()> {
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

/// Resolves on SIGTERM (what Kubernetes sends) or SIGINT.
#[cfg(unix)]
pub async fn wait_for_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = match signal(SignalKind::terminate()) {
        Ok(signal) => signal,
        Err(error) => {
            error!(error = %error, "Failed to install SIGTERM handler");
            return;
        }
    };
    let mut interrupt = match signal(SignalKind::interrupt()) {
        Ok(signal) => signal,
        Err(error) => {
            error!(error = %error, "Failed to install SIGINT handler");
            return;
        }
    };

    let signal_name = tokio::select! {
        _ = terminate.recv() => "SIGTERM",
        _ = interrupt.recv() => "SIGINT",
    };
    info!(signal = signal_name, "Received termination signal");
}

#[cfg(not(unix))]
pub async fn wait_for_signal() {
    if tokio::signal::ctrl_c().await.is_ok() {
        info!(signal = "CTRL_C", "Received termination signal");
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
            grace: Duration::from_secs(5),
        }
    }

    fn open_storage(dir: &tempfile::TempDir) -> StorageHandle {
        aruna_storage::FjallStorage::open(dir.path().to_str().expect("utf8 path"))
            .expect("storage opens")
    }

    #[tokio::test]
    async fn phase_gives_up_at_budget() {
        let started = Instant::now();

        phase("stuck", Duration::from_millis(50), std::future::pending()).await;

        assert!(started.elapsed() < Duration::from_secs(1));
    }

    // The inbound drain must never get the whole net phase, or the teardown
    // behind it is dropped by the outer phase timeout.
    #[test]
    fn drain_below_phase_budget() {
        assert_eq!(
            net_drain_budget(Duration::from_secs(10)),
            Duration::from_secs(8)
        );
        assert_eq!(net_drain_budget(Duration::from_secs(1)), Duration::ZERO);
    }

    // Every draining phase must leave the tail phases their floor.
    #[test]
    fn phases_reserve_tail() {
        assert_eq!(
            tail_reserved(Duration::from_secs(20)),
            Duration::from_secs(15)
        );
        assert_eq!(tail_reserved(Duration::from_secs(3)), Duration::ZERO);
    }

    #[tokio::test]
    async fn budget_shrinks_with_elapsed_time() {
        let budget = Budget::new(Instant::now(), Duration::from_millis(100));
        tokio::time::sleep(Duration::from_millis(120)).await;

        assert_eq!(budget.remaining(), Duration::ZERO);
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

    // SIGTERM is what Kubernetes sends; it must reach the same path as Ctrl-C.
    #[cfg(unix)]
    #[tokio::test]
    async fn sigterm_triggers_shutdown() {
        use std::process::Command;
        use tokio::signal::unix::{SignalKind, signal};

        // Registering here first replaces the process-wide default terminate
        // action, so the kill below cannot abort the test harness.
        let mut installed = signal(SignalKind::terminate()).expect("SIGTERM handler installs");
        let waiter = tokio::spawn(wait_for_signal());
        tokio::time::sleep(Duration::from_millis(100)).await;

        Command::new("kill")
            .arg("-TERM")
            .arg(std::process::id().to_string())
            .status()
            .expect("kill runs");

        tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("SIGTERM should end the signal wait")
            .expect("signal task joins");
        installed.recv().await;
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

        let started = Instant::now();
        sequence.run().await;

        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(storage_handle.is_sealed());
    }

    // A child that ignores cancellation must not hold up the sequence.
    #[tokio::test]
    async fn stuck_child_does_not_block() {
        let dir = tempdir().expect("temp dir");
        let storage_handle = open_storage(&dir);
        let shutdown = Shutdown::new();
        shutdown.spawn(std::future::pending());
        let mut sequence = node_shutdown(shutdown, storage_handle.clone());
        sequence.grace = Duration::from_millis(200);

        let started = Instant::now();
        sequence.run().await;

        assert!(started.elapsed() < Duration::from_secs(5));
        assert!(storage_handle.is_sealed());
    }
}
