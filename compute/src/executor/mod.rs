use aruna_core::compute::{
    AttemptRef, AttemptStatus, BackendError, CancelEvidence, ExecutorKind, LogLimits, LogTails,
    ReconcileOutcome, TaskOutput, TaskSpec,
};
use async_trait::async_trait;
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;

pub mod config;
pub mod logs;

#[cfg(feature = "docker")]
pub mod docker;

use logs::LogSink;

/// The single TES-shaped surface every backend is driven through. No backend
/// introduces a second state machine or data model.
#[async_trait]
pub trait ExecutorBackend: Send + Sync {
    fn kind(&self) -> ExecutorKind;

    /// Startup and advertisement gate.
    async fn health(&self) -> Result<(), BackendError>;

    /// Idempotent under the deterministic attempt name: a name collision MUST
    /// return the existing attempt's status, never start a second run.
    async fn submit(
        &self,
        spec: &TaskSpec,
        cancel: &CancellationToken,
    ) -> Result<AttemptStatus, BackendError>;

    async fn status(&self, attempt: &AttemptRef) -> Result<AttemptStatus, BackendError>;

    /// Wait for terminal evidence or the cancel token. Default impl polls
    /// `status()`; backends override with native waits.
    async fn wait(
        &self,
        attempt: &AttemptRef,
        cancel: &CancellationToken,
    ) -> Result<AttemptStatus, BackendError> {
        loop {
            if cancel.is_cancelled() {
                return self.status(attempt).await;
            }
            let status = self.status(attempt).await?;
            if status.is_terminal() {
                return Ok(status);
            }
            tokio::select! {
                _ = cancel.cancelled() => return self.status(attempt).await,
                _ = sleep(Duration::from_millis(500)) => {}
            }
        }
    }

    async fn cancel(&self, attempt: &AttemptRef) -> Result<CancelEvidence, BackendError>;

    /// Bounded tail per stream plus an optional full-stream copy into `sink`.
    async fn fetch_logs(
        &self,
        attempt: &AttemptRef,
        limits: &LogLimits,
        sink: &dyn LogSink,
    ) -> Result<LogTails, BackendError>;

    /// Stream one declared output file out of the terminal attempt.
    async fn fetch_output(
        &self,
        attempt: &AttemptRef,
        path: &str,
    ) -> Result<TaskOutput, BackendError>;

    /// Query by deterministic name after restart / lease loss. Never mutates.
    async fn reconcile(&self, attempt: &AttemptRef) -> ReconcileOutcome;

    /// Idempotently delete the external object. Called only after terminal
    /// evidence is durably recorded by the caller.
    async fn cleanup(&self, attempt: &AttemptRef) -> Result<(), BackendError>;
}
