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

#[cfg(feature = "kubernetes")]
pub mod kubernetes;

use logs::LogSink;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BackendCaps {
    pub file_staging: bool,
    pub direct_s3: bool,
}

#[cfg(any(feature = "apptainer", feature = "docker", feature = "kubernetes"))]
pub(crate) fn digest_pinned(image: &str) -> bool {
    image
        .rsplit_once("@sha256:")
        .is_some_and(|(repository, digest)| {
            !repository.is_empty()
                && digest.len() == 64
                && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
}

pub fn dispatch_helper() -> Option<i32> {
    let mode = std::env::args_os().nth(1)?;
    let mode = mode.to_str()?;
    if !matches!(mode, "apptainer-supervisor" | "payload-launcher") {
        return None;
    }
    #[cfg(feature = "apptainer")]
    {
        Some(apptainer::dispatch(mode))
    }
    #[cfg(not(feature = "apptainer"))]
    {
        Some(78)
    }
}

/// The single TES-shaped surface every backend is driven through. No backend
/// introduces a second state machine or data model.
#[async_trait]
pub trait ExecutorBackend: Send + Sync {
    fn kind(&self) -> ExecutorKind;

    fn capabilities(&self) -> BackendCaps {
        BackendCaps::default()
    }

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
