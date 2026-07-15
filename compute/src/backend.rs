use async_trait::async_trait;
use thiserror::Error;
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;

use crate::logs::{LogSink, LogTails};
use crate::spec::{AttemptRef, LogLimits, TaskSpec};
use crate::status::{AttemptStatus, CancelEvidence, ReconcileOutcome};

/// Executor kinds advertised on the Node Descriptor as a hard scheduling filter.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ExecutorKind {
    Docker,
    Kubernetes,
    Slurm,
    /// Namespaced extension kind, e.g. `x-firecracker`.
    Ext(String),
}

impl ExecutorKind {
    /// Wire form used on the Node Descriptor.
    pub fn as_wire(&self) -> String {
        match self {
            ExecutorKind::Docker => "docker".to_string(),
            ExecutorKind::Kubernetes => "kubernetes".to_string(),
            ExecutorKind::Slurm => "slurm".to_string(),
            ExecutorKind::Ext(name) => name.clone(),
        }
    }

    /// Parse a wire kind; unknown names become namespaced extensions.
    pub fn from_wire(value: &str) -> Self {
        match value {
            "docker" => ExecutorKind::Docker,
            "kubernetes" => ExecutorKind::Kubernetes,
            "slurm" => ExecutorKind::Slurm,
            other => ExecutorKind::Ext(other.to_string()),
        }
    }
}

/// Backend failure taxonomy. `retryable()` maps onto the Job error kind: a
/// permanent failure needs a new plan, a retryable one may re-attempt.
#[derive(Debug, Error, Clone)]
pub enum BackendError {
    #[error("image not found: {0}")]
    ImageNotFound(String),
    #[error("image access unauthorized: {0}")]
    ImageUnauthorized(String),
    #[error("invalid spec: {0}")]
    InvalidSpec(String),
    #[error("attempt not found: {0}")]
    NotFound(String),
    #[error("backend unavailable: {0}")]
    Unavailable(String),
    #[error("backend conflict: {0}")]
    Conflict(String),
    #[error("backend timeout: {0}")]
    Timeout(String),
    #[error("backend api error: {0}")]
    Api(String),
}

impl BackendError {
    /// Whether re-attempting the same plan could succeed. Bad input and missing
    /// images are permanent; transport faults and conflicts are retryable.
    pub fn retryable(&self) -> bool {
        match self {
            BackendError::ImageNotFound(_)
            | BackendError::ImageUnauthorized(_)
            | BackendError::InvalidSpec(_) => false,
            BackendError::NotFound(_)
            | BackendError::Unavailable(_)
            | BackendError::Conflict(_)
            | BackendError::Timeout(_)
            | BackendError::Api(_) => true,
        }
    }
}

/// The single TES-shaped surface every backend is driven through. No backend
/// introduces a second state machine or data model.
#[async_trait]
pub trait ExecutorBackend: Send + Sync {
    fn kind(&self) -> ExecutorKind;

    /// Startup and advertisement gate.
    async fn health(&self) -> Result<(), BackendError>;

    /// Idempotent under the deterministic attempt name: a name collision MUST
    /// return the existing attempt's status, never start a second run.
    async fn submit(&self, spec: &TaskSpec) -> Result<AttemptStatus, BackendError>;

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

    async fn fetch_output(&self, attempt: &AttemptRef, path: &str)
    -> Result<Vec<u8>, BackendError>;

    /// Query by deterministic name after restart / lease loss. Never mutates.
    async fn reconcile(&self, attempt: &AttemptRef) -> ReconcileOutcome;

    /// Idempotently delete the external object. Called only after terminal
    /// evidence is durably recorded by the caller.
    async fn cleanup(&self, attempt: &AttemptRef) -> Result<(), BackendError>;
}
