use aruna_core::compute::{
    AttemptStatus, BackendError, CancelEvidence, ExecutorKind, FenceContext, LogLimits, LogTails,
    ReconcileEvidence, TaskOutput, TaskSpec, TombstoneEvidence, TombstoneSpec,
};
use async_trait::async_trait;
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;

pub mod config;
pub mod logs;
pub mod staging;

#[cfg(feature = "apptainer")]
pub mod apptainer;

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

    async fn resolve_image(&self, image: &str) -> Result<String, BackendError>;

    async fn fence(&self, context: &FenceContext) -> Result<(), BackendError>;

    /// Idempotent under the deterministic attempt name: a name collision MUST
    /// return the existing attempt's status, never start a second run.
    async fn submit(
        &self,
        context: &FenceContext,
        spec: &TaskSpec,
        cancel: &CancellationToken,
    ) -> Result<AttemptStatus, BackendError>;

    async fn stage(
        &self,
        _context: &FenceContext,
        _spec: &TaskSpec,
        _cancel: &CancellationToken,
    ) -> Result<(), BackendError> {
        Err(BackendError::InvalidSpec(
            "backend does not support separate staging".to_string(),
        ))
    }

    async fn unsuspend(
        &self,
        _context: &FenceContext,
        _cancel: &CancellationToken,
    ) -> Result<AttemptStatus, BackendError> {
        Err(BackendError::InvalidSpec(
            "backend does not support unsuspend".to_string(),
        ))
    }

    async fn status(&self, context: &FenceContext) -> Result<AttemptStatus, BackendError>;

    /// Wait for terminal evidence or the cancel token. Default impl polls
    /// `status()`; backends override with native waits.
    async fn wait(
        &self,
        context: &FenceContext,
        cancel: &CancellationToken,
    ) -> Result<AttemptStatus, BackendError> {
        loop {
            if cancel.is_cancelled() {
                return self.status(context).await;
            }
            let status = self.status(context).await?;
            if status.is_terminal() {
                return Ok(status);
            }
            tokio::select! {
                _ = cancel.cancelled() => return self.status(context).await,
                _ = sleep(Duration::from_millis(500)) => {}
            }
        }
    }

    async fn cancel(&self, context: &FenceContext) -> Result<CancelEvidence, BackendError>;

    /// Bounded tail per stream plus an optional full-stream copy into `sink`.
    async fn fetch_logs(
        &self,
        context: &FenceContext,
        limits: &LogLimits,
        sink: &dyn LogSink,
    ) -> Result<LogTails, BackendError>;

    /// Stream one declared output file out of the terminal attempt.
    async fn fetch_output(
        &self,
        context: &FenceContext,
        path: &str,
    ) -> Result<TaskOutput, BackendError>;

    /// Query by deterministic name after restart / lease loss. Never mutates.
    async fn reconcile(&self, context: &FenceContext) -> ReconcileEvidence;

    async fn tombstone(
        &self,
        _context: &FenceContext,
        _spec: &TombstoneSpec,
    ) -> Result<TombstoneEvidence, BackendError> {
        Err(BackendError::InvalidSpec(
            "backend does not support tombstones".to_string(),
        ))
    }

    /// Idempotently delete the external object. Called only after terminal
    /// evidence is durably recorded by the caller.
    async fn cleanup(&self, context: &FenceContext) -> Result<(), BackendError>;

    async fn sweep_orphans(&self, _grace: Duration) -> Result<(), BackendError> {
        Ok(())
    }
}
