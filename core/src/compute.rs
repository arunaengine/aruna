use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::Duration;

use bytes::Bytes;
use futures::Stream;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const MAX_TRANSFER_BYTES: u64 = 4 * 1024 * 1024 * 1024;

pub type InputStream = Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send>>;
pub type OutputChunks = Pin<Box<dyn Stream<Item = Result<Bytes, BackendError>> + Send + Sync>>;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ExecutorKind {
    Docker,
    Apptainer,
    Kubernetes,
    Slurm,
    Ext(String),
}

impl ExecutorKind {
    pub fn as_wire(&self) -> String {
        match self {
            ExecutorKind::Docker => "docker".to_string(),
            ExecutorKind::Apptainer => "apptainer".to_string(),
            ExecutorKind::Kubernetes => "kubernetes".to_string(),
            ExecutorKind::Slurm => "slurm".to_string(),
            ExecutorKind::Ext(name) => name.clone(),
        }
    }

    pub fn from_wire(value: &str) -> Self {
        match value {
            "docker" => ExecutorKind::Docker,
            "apptainer" => ExecutorKind::Apptainer,
            "kubernetes" => ExecutorKind::Kubernetes,
            "slurm" => ExecutorKind::Slurm,
            other => ExecutorKind::Ext(other.to_string()),
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum BackendError {
    #[error("image not found: {0}")]
    ImageNotFound(String),
    #[error("image access unauthorized: {0}")]
    ImageUnauthorized(String),
    #[error("invalid spec: {0}")]
    InvalidSpec(String),
    #[error("backend submission cancelled")]
    Cancelled,
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
    #[error("controller generation is fenced")]
    Fenced,
}

impl BackendError {
    pub fn retryable(&self) -> bool {
        match self {
            BackendError::ImageNotFound(_)
            | BackendError::ImageUnauthorized(_)
            | BackendError::InvalidSpec(_)
            | BackendError::Cancelled
            | BackendError::Fenced => false,
            BackendError::NotFound(_)
            | BackendError::Unavailable(_)
            | BackendError::Conflict(_)
            | BackendError::Timeout(_)
            | BackendError::Api(_) => true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptRef {
    pub job_id: String,
    pub attempt: u32,
}

impl AttemptRef {
    pub fn new(job_id: impl Into<String>, attempt: u32) -> Self {
        Self {
            job_id: job_id.into(),
            attempt,
        }
    }

    pub fn external_name(&self) -> String {
        format!("aruna-{}-a{}", self.job_id.to_lowercase(), self.attempt)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.job_id.is_empty() {
            return Err("job_id is empty".to_string());
        }
        let valid = self
            .job_id
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '.' | '_' | '-'));
        if !valid {
            return Err(format!(
                "job_id `{}` contains characters outside [a-z0-9._-]",
                self.job_id
            ));
        }
        Ok(())
    }

    pub fn labels(&self) -> BTreeMap<String, String> {
        BTreeMap::from([
            ("aruna-engine.org/job-id".to_string(), self.job_id.clone()),
            (
                "aruna-engine.org/attempt".to_string(),
                self.attempt.to_string(),
            ),
        ])
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FenceContext {
    pub attempt: AttemptRef,
    pub attempt_epoch: u64,
    pub controller_generation: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceRequest {
    pub cpu_cores: Option<u32>,
    pub ram_bytes: Option<u64>,
    pub disk_bytes: Option<u64>,
    pub max_walltime: Option<Duration>,
    pub preemptible: bool,
    pub backend_extensions: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceBinding {
    pub s3_endpoint: String,
    pub bucket_name: String,
    pub region: String,
}

pub struct TaskInput {
    pub path: String,
    size: u64,
    stream: Mutex<Option<InputStream>>,
}

impl TaskInput {
    pub fn from_stream(path: impl Into<String>, size: u64, stream: InputStream) -> Self {
        Self {
            path: path.into(),
            size,
            stream: Mutex::new(Some(stream)),
        }
    }

    pub fn from_bytes(path: impl Into<String>, bytes: impl Into<Bytes>) -> Self {
        let bytes = bytes.into();
        let size = bytes.len() as u64;
        Self::from_stream(path, size, Box::pin(futures::stream::iter([Ok(bytes)])))
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn take_stream(&self) -> Option<InputStream> {
        self.stream.lock().ok()?.take()
    }
}

impl fmt::Debug for TaskInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskInput")
            .field("path", &self.path)
            .field("size", &self.size)
            .finish_non_exhaustive()
    }
}

impl PartialEq for TaskInput {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path && self.size == other.size
    }
}

impl Eq for TaskInput {}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Secret(String);

impl Secret {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(***)")
    }
}

impl Drop for Secret {
    fn drop(&mut self) {
        let bytes = unsafe { self.0.as_mut_vec() };
        let capacity = bytes.capacity();
        let ptr = bytes.as_mut_ptr();
        for index in 0..capacity {
            unsafe { std::ptr::write_volatile(ptr.add(index), 0) };
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogLimits {
    pub max_bytes_per_stream: usize,
    pub inline_tail_bytes: usize,
}

impl Default for LogLimits {
    fn default() -> Self {
        Self {
            max_bytes_per_stream: 256 * 1024,
            inline_tail_bytes: 8 * 1024,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct TaskSpec {
    pub attempt: AttemptRef,
    pub image: String,
    pub entrypoint: Option<Vec<String>>,
    pub command: Vec<String>,
    pub workdir: Option<String>,
    pub inputs: Vec<TaskInput>,
    pub output_paths: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub secret_env: BTreeMap<String, Secret>,
    pub resources: ResourceRequest,
    pub workspace: Option<WorkspaceBinding>,
    pub log_limits: LogLimits,
}

impl TaskSpec {
    pub fn new(attempt: AttemptRef, image: impl Into<String>) -> Self {
        Self {
            attempt,
            image: image.into(),
            entrypoint: None,
            command: Vec::new(),
            workdir: None,
            inputs: Vec::new(),
            output_paths: Vec::new(),
            env: BTreeMap::new(),
            secret_env: BTreeMap::new(),
            resources: ResourceRequest::default(),
            workspace: None,
            log_limits: LogLimits::default(),
        }
    }

    pub fn effective_env(&self) -> BTreeMap<String, String> {
        let mut env = self.env.clone();
        if let Some(workspace) = &self.workspace {
            env.insert(
                "AWS_ENDPOINT_URL".to_string(),
                workspace.s3_endpoint.clone(),
            );
            env.insert("AWS_REGION".to_string(), workspace.region.clone());
            env.insert(
                "ARUNA_WORKSPACE_BUCKET".to_string(),
                workspace.bucket_name.clone(),
            );
        }
        env.insert("ARUNA_JOB_ID".to_string(), self.attempt.job_id.clone());
        for (key, value) in &self.secret_env {
            env.insert(key.clone(), value.expose().to_string());
        }
        env
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AttemptPhase {
    Submitted,
    Running,
    Exited { code: i32 },
    Failed { reason: String },
    Cancelled,
}

impl AttemptPhase {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            AttemptPhase::Exited { .. } | AttemptPhase::Failed { .. } | AttemptPhase::Cancelled
        )
    }

    pub fn tes_state(&self) -> TesState {
        match self {
            AttemptPhase::Submitted => TesState::Initializing,
            AttemptPhase::Running => TesState::Running,
            AttemptPhase::Exited { code: 0 } => TesState::Complete,
            AttemptPhase::Exited { .. } => TesState::ExecutorError,
            AttemptPhase::Failed { .. } => TesState::SystemError,
            AttemptPhase::Cancelled => TesState::Canceled,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TesState {
    Queued,
    Initializing,
    Running,
    Complete,
    ExecutorError,
    SystemError,
    Canceled,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptStatus {
    pub phase: AttemptPhase,
    pub backend_ref: String,
    pub started_at_ms: Option<u64>,
    pub finished_at_ms: Option<u64>,
}

impl AttemptStatus {
    pub fn is_terminal(&self) -> bool {
        self.phase.is_terminal()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResumePoint {
    Observe,
    Submit,
    Stage,
    Unsuspend,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdoptableEvidence {
    pub status: AttemptStatus,
    pub resume: ResumePoint,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactEvidence {
    pub artifact_kind: String,
    pub backend_ref: Option<String>,
    pub observed_epoch: Option<u64>,
    pub observed_generation: Option<u64>,
    pub exact_identity: bool,
    pub multiple: bool,
    pub foreign: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ReconcileEvidence {
    Adoptable(AdoptableEvidence),
    Unadoptable(ArtifactEvidence),
    Absent,
    Tombstoned(TombstoneEvidence),
    Unavailable(BackendError),
}

#[derive(Debug, PartialEq, Eq)]
pub enum CancelEvidence {
    Stopped(AttemptStatus),
    Requested,
    AlreadyGone,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LogTails {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub stdout_total: u64,
    pub stderr_total: u64,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogStream {
    Stdout,
    Stderr,
}

pub struct TaskOutput {
    pub size: u64,
    pub chunks: OutputChunks,
}

impl fmt::Debug for TaskOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskOutput")
            .field("size", &self.size)
            .finish_non_exhaustive()
    }
}

impl PartialEq for TaskOutput {
    fn eq(&self, other: &Self) -> bool {
        self.size == other.size
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TombstoneSpec {
    pub terminal_ref: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TombstoneEvidence {
    pub backend_ref: String,
    pub attempt_epoch: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ComputeEffect {
    ResolveImage {
        backend: ExecutorKind,
        image: String,
    },
    Fence {
        backend: ExecutorKind,
        context: FenceContext,
    },
    Submit {
        backend: ExecutorKind,
        context: FenceContext,
        spec: TaskSpec,
    },
    Stage {
        backend: ExecutorKind,
        context: FenceContext,
        spec: TaskSpec,
    },
    Unsuspend {
        backend: ExecutorKind,
        context: FenceContext,
    },
    Status {
        backend: ExecutorKind,
        context: FenceContext,
    },
    Wait {
        backend: ExecutorKind,
        context: FenceContext,
    },
    Cancel {
        backend: ExecutorKind,
        context: FenceContext,
    },
    FetchLogs {
        backend: ExecutorKind,
        context: FenceContext,
        limits: LogLimits,
    },
    FetchOutput {
        backend: ExecutorKind,
        context: FenceContext,
        path: String,
    },
    Reconcile {
        backend: ExecutorKind,
        context: FenceContext,
    },
    Tombstone {
        backend: ExecutorKind,
        context: FenceContext,
        spec: TombstoneSpec,
    },
    Cleanup {
        backend: ExecutorKind,
        context: FenceContext,
    },
    Sweep {
        backend: ExecutorKind,
        grace: Duration,
    },
}

#[derive(Debug, PartialEq)]
pub enum ComputeEvent {
    ImageResolved(Result<String, BackendError>),
    Fenced(Result<(), BackendError>),
    Submitted(Result<AttemptStatus, BackendError>),
    Staged(Result<(), BackendError>),
    Unsuspended(Result<AttemptStatus, BackendError>),
    Status(Result<AttemptStatus, BackendError>),
    Waited(Result<AttemptStatus, BackendError>),
    Cancelled(Result<CancelEvidence, BackendError>),
    LogsFetched(Result<LogTails, BackendError>),
    OutputFetched(Result<TaskOutput, BackendError>),
    Reconciled(ReconcileEvidence),
    Tombstoned(Result<TombstoneEvidence, BackendError>),
    Cleaned(Result<(), BackendError>),
    Swept(Result<(), BackendError>),
}
