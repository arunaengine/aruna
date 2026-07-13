use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aruna_core::structs::{JobError, JobPayload, JobProgress, JobResultPayload};
use tokio_util::sync::CancellationToken;

use crate::driver::DriverContext;

/// In-memory progress with a persisted snapshot taken on flush and lease renew.
#[derive(Clone)]
pub struct ProgressReporter {
    inner: Arc<ProgressInner>,
}

struct ProgressInner {
    current: AtomicU64,
    total: Mutex<Option<u64>>,
    unit: String,
}

impl ProgressReporter {
    pub fn from_progress(progress: &JobProgress) -> Self {
        Self {
            inner: Arc::new(ProgressInner {
                current: AtomicU64::new(progress.current),
                total: Mutex::new(progress.total),
                unit: progress.unit.clone(),
            }),
        }
    }

    pub fn set_total(&self, total: u64) {
        *self.inner.total.lock().expect("progress mutex poisoned") = Some(total);
    }

    pub fn advance(&self, delta: u64) {
        self.inner.current.fetch_add(delta, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> JobProgress {
        JobProgress {
            current: self.inner.current.load(Ordering::Relaxed),
            total: *self.inner.total.lock().expect("progress mutex poisoned"),
            unit: self.inner.unit.clone(),
        }
    }
}

pub struct JobContext {
    pub driver: Arc<DriverContext>,
    /// User-initiated cancel: terminal `Cancelled` plus cleanup.
    pub cancel: CancellationToken,
    /// Node shutdown: stop where you are, the lease is handed back and the job re-runs.
    pub shutdown: CancellationToken,
    pub progress: ProgressReporter,
}

pub enum JobRunOutcome {
    Succeeded(JobResultPayload),
    Failed(JobError),
    Cancelled,
    Interrupted,
}

/// Payload dispatch, mirroring how the task handler matches `TaskKey`. Execution
/// payloads never reach here: the runtime routes them to the fenced external path
/// before this seam.
pub async fn dispatch_payload(ctx: &JobContext, payload: &JobPayload) -> JobRunOutcome {
    match payload {
        JobPayload::Probe {
            steps,
            step_sleep_ms,
            fail_at,
            panic_at,
            cleanup_marker,
        } => {
            run_probe(
                ctx,
                *steps,
                *step_sleep_ms,
                *fail_at,
                *panic_at,
                cleanup_marker.as_deref(),
            )
            .await
        }
        JobPayload::WriteRunCrate { for_job } => {
            crate::jobs::workflow::run_crate::run_write_run_crate(ctx, *for_job).await
        }
        // Guard: an execution job must run through the external attempt path.
        JobPayload::Execution(_) => JobRunOutcome::Failed(JobError::permanent(
            "execution payload dispatched through the in-process seam",
        )),
    }
}

/// Idempotent cleanup hook run on the cancellation path. Execution/crate payloads
/// carry no in-process side effects here.
pub fn run_cleanup(payload: &JobPayload) {
    match payload {
        JobPayload::Probe { cleanup_marker, .. } => {
            if let Some(marker) = cleanup_marker {
                let _ = std::fs::remove_file(marker);
            }
        }
        JobPayload::Execution(_) | JobPayload::WriteRunCrate { .. } => {}
    }
}

async fn run_probe(
    ctx: &JobContext,
    steps: u32,
    step_sleep_ms: u64,
    fail_at: Option<u32>,
    panic_at: Option<u32>,
    cleanup_marker: Option<&str>,
) -> JobRunOutcome {
    ctx.progress.set_total(steps as u64);
    if let Some(marker) = cleanup_marker {
        let _ = std::fs::write(marker, b"running");
    }
    for step in 0..steps {
        // A zero-sleep probe never awaits otherwise, so it would starve everything else
        // sharing its worker until the whole run finished.
        tokio::task::yield_now().await;
        if ctx.cancel.is_cancelled() {
            return JobRunOutcome::Cancelled;
        }
        if ctx.shutdown.is_cancelled() {
            return JobRunOutcome::Interrupted;
        }
        if panic_at == Some(step) {
            panic!("probe panic at step {step}");
        }
        if fail_at == Some(step) {
            return JobRunOutcome::Failed(JobError::retryable(format!(
                "probe failed at step {step}"
            )));
        }
        if step_sleep_ms > 0 {
            tokio::select! {
                biased;
                _ = ctx.cancel.cancelled() => return JobRunOutcome::Cancelled,
                _ = ctx.shutdown.cancelled() => return JobRunOutcome::Interrupted,
                _ = tokio::time::sleep(Duration::from_millis(step_sleep_ms)) => {}
            }
        }
        ctx.progress.advance(1);
    }
    if let Some(marker) = cleanup_marker {
        let _ = std::fs::remove_file(marker);
    }
    JobRunOutcome::Succeeded(JobResultPayload::Probe {
        completed_steps: steps,
    })
}
