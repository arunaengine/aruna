use aruna_core::compute::{
    AttemptStatus, BackendError, CancelEvidence, ExecutorKind, FenceContext, LogLimits, LogTails,
    ReconcileEvidence, ResourceEnvelope, TaskOutput, TaskSpec, TombstoneEvidence, TombstoneSpec,
    UserSpec,
};
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::pin::Pin;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;

#[cfg(any(feature = "apptainer", feature = "docker", feature = "kubernetes"))]
pub(crate) mod channel;

#[cfg(any(feature = "apptainer", feature = "docker"))]
pub(crate) mod control_store;

pub mod config;
pub mod logs;
pub mod staging;

#[cfg(feature = "apptainer")]
pub mod apptainer;

#[cfg(feature = "docker")]
pub mod docker;

#[cfg(feature = "kubernetes")]
pub mod kubernetes;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BackendCaps {
    pub file_staging: bool,
    pub direct_s3: bool,
    pub s3_mount: bool,
    /// The backend proves where workers run and enforces their network
    /// isolation, which protected data requires before open networking.
    pub network_policy: bool,
    /// The workload runs on the controller host, so it inherits the
    /// controller's execution subject.
    pub local_site: bool,
    /// Where a non-local backend's workers run. `None` means their placement is
    /// unproven, so the advertisement carries no site details at all.
    pub worker_site: Option<WorkerSite>,
    /// Static ceilings; `None` is unmeasured and never filters.
    pub limits: ResourceEnvelope,
    /// The backend can open a byte channel to a running attempt, which an
    /// interactive session needs.
    pub session: bool,
}

/// One bidirectional byte channel to the helper inside a running attempt. The
/// node forwards bytes over it and never interprets what a cell contains.
pub struct SessionChannel {
    pub input: Pin<Box<dyn AsyncWrite + Send>>,
    pub output: Pin<Box<dyn AsyncRead + Send>>,
}

/// The operator-declared execution site of workers that do not run on the
/// controller host.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkerSite {
    pub location: String,
    pub labels: BTreeMap<String, String>,
}

/// The ceiling one attempt runs under: the stored request's own, else the
/// backend default. A dimension with neither is unbounded, which is never a
/// legal execution envelope.
pub fn enforced_limit(
    stored: Option<u64>,
    default: Option<u64>,
    dimension: &str,
) -> Result<u64, BackendError> {
    match stored.or(default).filter(|limit| *limit > 0) {
        Some(limit) => Ok(limit),
        None => Err(BackendError::InvalidSpec(format!(
            "attempt has no {dimension} ceiling and the backend configures none"
        ))),
    }
}

/// Wall-clock milliseconds every backend stamps its attempt evidence with.
pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
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
        eprintln!("Apptainer helper mode requires the apptainer feature");
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

    /// The identity tasks actually run as, which the task manifest reports.
    /// Backends that perform a real user switch pin it; backends that perform
    /// none MUST report the service's own identity rather than a wish.
    fn run_identity(&self) -> UserSpec;

    /// Startup and advertisement gate.
    async fn health(&self) -> Result<(), BackendError>;

    async fn resolve_image(
        &self,
        image: &str,
        cancel: &CancellationToken,
    ) -> Result<String, BackendError>;

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

    /// Bounded tail per stream.
    async fn fetch_logs(
        &self,
        context: &FenceContext,
        limits: &LogLimits,
    ) -> Result<LogTails, BackendError>;

    /// Stream one declared output file out of the terminal attempt.
    async fn fetch_output(
        &self,
        context: &FenceContext,
        path: &str,
    ) -> Result<TaskOutput, BackendError>;

    /// Resolve a declared POSIX output pattern against the terminal attempt's
    /// filesystem. Zero matches is an empty list, never an error.
    async fn list_outputs(
        &self,
        _context: &FenceContext,
        _pattern: &str,
    ) -> Result<Vec<String>, BackendError> {
        Err(BackendError::InvalidSpec(
            "backend does not support wildcard outputs".to_string(),
        ))
    }

    /// Open a byte channel to the session helper of a running attempt. Only a
    /// backend advertising `BackendCaps.session` implements it. A reconnect
    /// opens a new channel; nothing is resumed inside the container.
    async fn open_session(&self, _context: &FenceContext) -> Result<SessionChannel, BackendError> {
        Err(BackendError::InvalidSpec(
            "backend does not support session channels".to_string(),
        ))
    }

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
}
