use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Aggregate ceiling for in-memory task file-transfer payloads.
// Real-file streaming is a follow-up; transfers remain memory-buffered for now.
pub const MAX_TRANSFER_BYTES: usize = 4 * 1024 * 1024 * 1024;

/// Fence identity of one attempt. Deterministically names the external object
/// via [`AttemptRef::external_name`]; that name is the reconciliation key.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptRef {
    /// Caller-supplied stable job name. Must be lowercase `[a-z0-9._-]` so it
    /// maps injectively onto backend object names.
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

    /// Deterministic external object name shared by every backend.
    pub fn external_name(&self) -> String {
        format!("aruna-{}-a{}", self.job_id.to_lowercase(), self.attempt)
    }

    /// Reject job ids that would not map onto a valid backend object name. Only
    /// lowercase ids are accepted: `external_name` folds case, so `JobA` and
    /// `joba` would otherwise collide on one container name.
    pub fn validate(&self) -> Result<(), String> {
        if self.job_id.is_empty() {
            return Err("job_id is empty".to_string());
        }
        let ok = self
            .job_id
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '.' | '_' | '-'));
        if !ok {
            return Err(format!(
                "job_id `{}` contains characters outside [a-z0-9._-]",
                self.job_id
            ));
        }
        Ok(())
    }

    /// Reconciliation/forensics labels applied to the external object.
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

/// Resource ceilings for one attempt. A request without a ceiling is filled
/// from backend config defaults, never left unlimited.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceRequest {
    pub cpu_cores: Option<u32>,
    pub ram_bytes: Option<u64>,
    pub disk_bytes: Option<u64>,
    pub max_walltime: Option<Duration>,
    pub preemptible: bool,
    /// Namespaced backend-specific knobs (e.g. GPUs) with no first-class field.
    /// A backend that does not understand an extension must reject the spec
    /// rather than silently dropping it.
    pub backend_extensions: BTreeMap<String, String>,
}

/// S3 workspace the attempt reads inputs from and writes outputs to. Injected
/// as standard AWS SDK env so unconfigured tooling works.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceBinding {
    pub s3_endpoint: String,
    pub bucket_name: String,
    pub region: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskInput {
    pub path: String,
    pub contents: Vec<u8>,
}

/// Zeroized secret wrapper. Redacts on `Debug`; serializes transparently so a
/// caller that owns the plan can persist it, and round-trips in tests.
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
        // Zero the whole capacity, not just the live length: a String that grew leaves
        // secret bytes behind in the slack past `len`.
        let bytes = unsafe { self.0.as_mut_vec() };
        let capacity = bytes.capacity();
        let ptr = bytes.as_mut_ptr();
        for i in 0..capacity {
            unsafe { std::ptr::write_volatile(ptr.add(i), 0) };
        }
    }
}

/// Per-stream byte caps for bounded log capture.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogLimits {
    /// Ring-buffer tail retained per stream.
    pub max_bytes_per_stream: usize,
    /// Slice of the tail surfaced inline in the job API.
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

/// Everything a backend needs to run one attempt. The reconciliation key is
/// `attempt.external_name()`; the backend never reads any Aruna record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskSpec {
    pub attempt: AttemptRef,
    /// Digest-pinned form recorded for provenance.
    pub image: String,
    /// Overrides the image ENTRYPOINT when set; `None` keeps the image default.
    pub entrypoint: Option<Vec<String>>,
    pub command: Vec<String>,
    pub workdir: Option<String>,
    pub inputs: Vec<TaskInput>,
    pub output_paths: Vec<String>,
    /// Non-secret environment.
    pub env: BTreeMap<String, String>,
    /// Secret environment (workflow S3 credentials); zeroized wrapper.
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

    /// Full env (non-secret + secret + workspace AWS vars) a backend injects.
    pub fn effective_env(&self) -> BTreeMap<String, String> {
        let mut env = self.env.clone();
        if let Some(ws) = &self.workspace {
            env.insert("AWS_ENDPOINT_URL".to_string(), ws.s3_endpoint.clone());
            env.insert("AWS_REGION".to_string(), ws.region.clone());
            env.insert("ARUNA_WORKSPACE_BUCKET".to_string(), ws.bucket_name.clone());
        }
        env.insert("ARUNA_JOB_ID".to_string(), self.attempt.job_id.clone());
        for (k, v) in &self.secret_env {
            env.insert(k.clone(), v.expose().to_string());
        }
        env
    }
}
