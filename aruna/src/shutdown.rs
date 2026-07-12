//! Ordered node shutdown.
//!
//! One cancellation path (SIGTERM, SIGINT, or a subsystem failure) drives a
//! fixed sequence of phases. Each phase is bounded, the whole sequence is
//! bounded, and a watchdog thread force-exits the process if even that hangs.
//!
//! The order is dictated by what writes to what:
//!
//! 1. Readiness flips to draining. Kubernetes stops routing new requests here
//!    before anything is torn down. Liveness keeps answering for the whole
//!    sequence, so the kubelet does not SIGKILL us mid-drain.
//! 2. Ingress (REST, S3) stops accepting and finishes in-flight requests. New
//!    writes can no longer enter the node.
//! 3. Timer handlers drain while the network is still up: outbox publishing and
//!    notification delivery need peers. Whatever does not finish stays durable
//!    in storage and is re-armed on the next boot.
//! 4. Background children (metadata maintenance, reconcile runs, gauges) are
//!    cancelled and joined; they write metadata and storage.
//! 5. The network shuts down. Its eviction path re-emits documents through the
//!    still-registered inbound handler, so the handler is only cleared after.
//! 6. The metadata store is flushed.
//! 7. Storage is sealed, then synced. Nothing can commit after the seal, so a
//!    leaked child cannot write behind the final fsync.

use std::sync::mpsc::{RecvTimeoutError, SyncSender, sync_channel};
use std::thread;
use std::time::{Duration, Instant};

use aruna_api::error::ServerSetupError;
use aruna_api::ops::Readiness;
use aruna_core::shutdown::Shutdown;
use aruna_net::NetHandle;
use aruna_operations::metadata::MetadataHandle;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

/// Total budget for the ordered sequence. Kubernetes sends SIGKILL after
/// `terminationGracePeriodSeconds` (30s by default), so stay clear of it.
pub const DEFAULT_SHUTDOWN_GRACE: Duration = Duration::from_secs(20);
/// Extra time the watchdog allows before it stops trusting the sequence.
const WATCHDOG_MARGIN: Duration = Duration::from_secs(5);
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
    pub net_handle: Option<NetHandle>,
    pub metadata_handle: Option<MetadataHandle>,
    pub storage_handle: StorageHandle,
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
        phase("ingress", budget.remaining(), async {
            if let Some(rest) = self.rest {
                let _ = rest.await;
            }
            if let Some(s3) = self.s3 {
                let _ = s3.await;
            }
        })
        .await;

        // 3. Timer handlers drain while the network still works.
        let drain = budget.remaining();
        let report = self.task_handle.shutdown(drain).await;
        info!(
            in_flight = report.in_flight,
            aborted = report.aborted,
            "Shutdown: task scheduler drained"
        );

        // 4. Background children write metadata and storage: join them.
        phase("background", budget.remaining(), async {
            self.shutdown.drain(budget.remaining()).await;
        })
        .await;

        // 5. Network last among the writers: its eviction path re-emits
        //    documents through the inbound handler.
        if let Some(net_handle) = self.net_handle.as_ref() {
            phase("net", budget.remaining(), async {
                net_handle.shutdown_with_drain(budget.remaining()).await;
            })
            .await;
            net_handle.clear_inbound_handler();
        }

        // 6. Flush the metadata store.
        if let Some(metadata_handle) = self.metadata_handle.as_ref() {
            phase("metadata", budget.remaining(), async {
                if let Err(error) = metadata_handle.flush_persistence().await {
                    error!(error = %error, "Failed to flush metadata persistence during shutdown");
                }
            })
            .await;
        }

        // 7. Close the write path, then fsync. Nothing commits after this.
        self.storage_handle.seal();
        if let Err(error) = self.storage_handle.sync_all().await {
            error!(error = %error, "Failed to sync storage during shutdown");
        }
        let rejected = self.storage_handle.rejected_writes();
        if rejected > 0 {
            warn!(
                rejected,
                "Rejected storage writes issued after the shutdown seal"
            );
        }

        info!(
            elapsed_ms = started.elapsed().as_millis(),
            rejected_writes = rejected,
            "Shutdown complete"
        );
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tempfile::tempdir;

    fn node_shutdown(shutdown: Shutdown, storage_handle: StorageHandle) -> NodeShutdown {
        NodeShutdown {
            shutdown,
            readiness: Readiness::new(),
            rest: None,
            s3: None,
            task_handle: TaskHandle::new(),
            net_handle: None,
            metadata_handle: None,
            storage_handle,
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
    async fn shutdown_joins_background_children() {
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

    // A child that ignores cancellation must not hold up the sequence.
    #[tokio::test]
    async fn stuck_child_does_not_block_seal() {
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
