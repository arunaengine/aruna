use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::pin::Pin;
use std::sync::Mutex;
use std::time::Duration;

use bytes::Bytes;
use futures::Stream;
use globset::{Glob, GlobBuilder, GlobMatcher, GlobSet, GlobSetBuilder};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use zeroize::Zeroize;

pub const MAX_TRANSFER_BYTES: u64 = 4 * 1024 * 1024 * 1024;

/// Pattern characters of IEEE Std 1003.1-2017 (POSIX) 12.13, the only wildcards
/// TES 1.1 allows in `tesOutput.path`.
const WILDCARDS: [char; 3] = ['*', '?', '['];

/// Upper bound on the files a single wildcard output may expand to.
pub const MAX_OUTPUT_MATCHES: usize = 1024;

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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutorCapability {
    pub kind: String,
    pub file_staging: bool,
    pub direct_s3: bool,
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3Mount {
    pub bucket: String,
    pub key: String,
    pub path: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StagingMode {
    Files,
    DirectS3,
    S3Mount,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserSpec {
    pub uid: u32,
    pub gid: u32,
}

/// The unprivileged identity backends pin when they perform a real user switch.
pub const NOBODY: UserSpec = UserSpec {
    uid: 65_534,
    gid: 65_534,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NetworkAccess {
    Isolated,
    S3Only,
    Open,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecurityContext {
    pub run_as: UserSpec,
    pub drop_all_caps: bool,
    pub no_new_privileges: bool,
    pub network: NetworkAccess,
    pub read_only_rootfs: bool,
    pub pids_limit: Option<u64>,
    pub seccomp_default: bool,
}

impl Default for SecurityContext {
    fn default() -> Self {
        Self {
            run_as: NOBODY,
            drop_all_caps: true,
            no_new_privileges: true,
            network: NetworkAccess::Isolated,
            read_only_rootfs: false,
            pids_limit: Some(2048),
            seccomp_default: true,
        }
    }
}

pub struct TaskInput {
    pub path: String,
    pub workspace_key: String,
    size: u64,
    stream: Mutex<Option<InputStream>>,
}

impl TaskInput {
    pub fn from_stream(path: impl Into<String>, size: u64, stream: InputStream) -> Self {
        let path = path.into();
        Self {
            workspace_key: path.clone(),
            path,
            size,
            stream: Mutex::new(Some(stream)),
        }
    }

    pub fn from_workspace(
        path: impl Into<String>,
        workspace_key: impl Into<String>,
        size: u64,
        stream: InputStream,
    ) -> Self {
        Self {
            path: path.into(),
            workspace_key: workspace_key.into(),
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
            .field("workspace_key", &self.workspace_key)
            .field("size", &self.size)
            .finish_non_exhaustive()
    }
}

impl PartialEq for TaskInput {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
            && self.workspace_key == other.workspace_key
            && self.size == other.size
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
        self.0.zeroize();
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
    pub s3_mounts: Vec<S3Mount>,
    pub staging_mode: StagingMode,
    pub output_paths: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub secret_env: BTreeMap<String, Secret>,
    pub resources: ResourceRequest,
    pub workspace: Option<WorkspaceBinding>,
    pub security: SecurityContext,
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
            s3_mounts: Vec::new(),
            staging_mode: StagingMode::Files,
            output_paths: Vec::new(),
            env: BTreeMap::new(),
            secret_env: BTreeMap::new(),
            resources: ResourceRequest::default(),
            workspace: None,
            security: SecurityContext::default(),
            log_limits: LogLimits::default(),
        }
    }

    pub fn effective_env(&self) -> BTreeMap<String, String> {
        let mut env = self.env.clone();
        env.insert("ARUNA_JOB_ID".to_string(), self.attempt.job_id.clone());
        if self.staging_mode == StagingMode::DirectS3 {
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
            for (key, value) in &self.secret_env {
                env.insert(key.clone(), value.expose().to_string());
            }
        }
        env
    }
}

pub fn normalize_container_path(path: &str) -> Result<PathBuf, String> {
    let path = Path::new(path);
    if !path.is_absolute() || path == Path::new("/") {
        return Err("container path must be absolute and non-root".to_string());
    }
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::RootDir => normalized.push("/"),
            Component::Normal(part) if !part.as_encoded_bytes().contains(&0) => {
                normalized.push(part)
            }
            _ => return Err("container path contains an unsafe component".to_string()),
        }
    }
    Ok(normalized)
}

pub fn paths_overlap(input: &str, output_parent: &str) -> Result<bool, String> {
    let input = normalize_container_path(input)?;
    let output_parent = normalize_container_path(output_parent)?;
    Ok(input == output_parent || output_parent.starts_with(&input))
}

/// True when a container path carries a POSIX 12.13 wildcard.
pub fn has_wildcard(path: &str) -> bool {
    path.contains(WILDCARDS)
}

/// POSIX gives a run of `*` the meaning of a single `*`, so `**` must not turn
/// into the recursive wildcard of the glob engine.
fn collapse_stars(pattern: &str) -> String {
    let mut collapsed = String::with_capacity(pattern.len());
    let (mut escaped, mut star) = (false, false);
    for character in pattern.chars() {
        match character {
            _ if escaped => (escaped, star) = (false, false),
            '\\' => (escaped, star) = (true, false),
            '*' if star => continue,
            '*' => star = true,
            _ => star = false,
        }
        collapsed.push(character);
    }
    collapsed
}

/// POSIX 12.13 has no brace alternation, so `{` and `}` stay literal file name
/// characters instead of expanding into alternates the caller never declared.
fn escape_braces(pattern: &str) -> String {
    let mut escaped = String::with_capacity(pattern.len());
    let (mut skip, mut class) = (false, false);
    for character in pattern.chars() {
        match character {
            _ if skip => skip = false,
            '\\' => skip = true,
            '[' if !class => class = true,
            ']' if class => class = false,
            '{' | '}' if !class => {
                escaped.push('[');
                escaped.push(character);
                escaped.push(']');
                continue;
            }
            _ => {}
        }
        escaped.push(character);
    }
    escaped
}

fn build_glob(pattern: &str) -> Result<Glob, String> {
    // `literal_separator` keeps `*` and `?` inside one path component, as POSIX requires.
    GlobBuilder::new(&escape_braces(&collapse_stars(pattern)))
        .literal_separator(true)
        .build()
        .map_err(|error| error.to_string())
}

/// Compile one output pattern into a matcher anchored at the absolute pattern.
pub fn output_glob(pattern: &str) -> Result<GlobMatcher, String> {
    normalize_container_path(pattern)?;
    Ok(build_glob(pattern)?.compile_matcher())
}

/// Deepest wildcard-free ancestor of a pattern, which is the directory a backend
/// enumerates. A wildcard-free path yields itself.
pub fn literal_prefix(pattern: &str) -> Result<PathBuf, String> {
    let normalized = normalize_container_path(pattern)?;
    let mut prefix = PathBuf::from("/");
    for component in normalized.components() {
        let Component::Normal(part) = component else {
            continue;
        };
        if part.to_string_lossy().contains(WILDCARDS) {
            break;
        }
        prefix.push(part);
    }
    Ok(prefix)
}

/// The part of `path` below `prefix`, split on a path-component boundary.
/// `None` when `prefix` is not a strict ancestor of `path`.
pub fn output_suffix(path: &str, prefix: &str) -> Option<String> {
    let path = normalize_container_path(path).ok()?;
    let prefix = normalize_container_path(prefix).ok()?;
    let suffix = path.strip_prefix(&prefix).ok()?.to_str()?;
    (!suffix.is_empty()).then(|| suffix.to_string())
}

/// The declared outputs of one attempt: a wildcard path selects every file it
/// matches, a wildcard-free path only itself.
#[derive(Debug)]
pub struct OutputMatcher {
    literals: Vec<String>,
    patterns: GlobSet,
}

impl OutputMatcher {
    pub fn new<'a>(declared: impl IntoIterator<Item = &'a str>) -> Result<Self, String> {
        let mut literals = Vec::new();
        let mut builder = GlobSetBuilder::new();
        for path in declared {
            normalize_container_path(path)?;
            if has_wildcard(path) {
                builder.add(build_glob(path)?);
            } else {
                literals.push(path.to_string());
            }
        }
        Ok(Self {
            literals,
            patterns: builder.build().map_err(|error| error.to_string())?,
        })
    }

    pub fn is_match(&self, path: &str) -> bool {
        self.literals.iter().any(|literal| literal == path) || self.patterns.is_match(path)
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

#[cfg(test)]
mod tests {
    use super::*;

    const LISTING: [&str; 6] = [
        "/out/a.txt",
        "/out/b.txt",
        "/out/ab.txt",
        "/out/report.csv",
        "/out/sub/c.txt",
        "/out/sub/deep/d.txt",
    ];

    fn matched(pattern: &str) -> Vec<&'static str> {
        let glob = output_glob(pattern).unwrap();
        LISTING
            .into_iter()
            .filter(|path| glob.is_match(path))
            .collect()
    }

    #[test]
    fn expands_posix_pattern() {
        // `*` and `?` must stay inside one path component; `[..]` is a class.
        assert_eq!(
            matched("/out/*.txt"),
            vec!["/out/a.txt", "/out/b.txt", "/out/ab.txt"]
        );
        assert_eq!(matched("/out/?.txt"), vec!["/out/a.txt", "/out/b.txt"]);
        assert_eq!(matched("/out/[ab].txt"), vec!["/out/a.txt", "/out/b.txt"]);
        assert_eq!(matched("/out/sub/*.txt"), vec!["/out/sub/c.txt"]);
        assert!(matched("/out/*.md").is_empty());
        // A repeated `*` stays one POSIX wildcard instead of recursing.
        assert_eq!(matched("/out/**/*.txt"), vec!["/out/sub/c.txt"]);
        assert!(output_glob("/out/[a.txt").is_err());
    }

    #[test]
    fn braces_stay_literal() {
        // Brace alternation is undeclared, so `{a,b}` names one directory.
        let glob = output_glob("/out/{a,b}/x.txt").unwrap();
        assert!(glob.is_match("/out/{a,b}/x.txt"));
        assert!(!glob.is_match("/out/a/x.txt"));
        assert!(!glob.is_match("/out/b/x.txt"));
        assert!(!has_wildcard("/out/{a,b}/x.txt"));
        assert_eq!(
            literal_prefix("/out/{a,b}/x.txt").unwrap(),
            Path::new("/out/{a,b}/x.txt")
        );

        let matcher = OutputMatcher::new(["/out/{a,b}.csv", "/out/[ab]{x}.txt"]).unwrap();
        assert!(matcher.is_match("/out/{a,b}.csv"));
        assert!(!matcher.is_match("/out/a.csv"));
        assert!(matcher.is_match("/out/a{x}.txt"));
        assert!(!matcher.is_match("/out/ax.txt"));
    }

    #[test]
    fn detects_wildcards() {
        assert!(has_wildcard("/out/*.txt"));
        assert!(has_wildcard("/out/?.txt"));
        assert!(has_wildcard("/out/[ab].txt"));
        assert!(!has_wildcard("/out/report.txt"));
    }

    #[test]
    fn finds_literal_prefix() {
        assert_eq!(literal_prefix("/out/*.txt").unwrap(), Path::new("/out"));
        assert_eq!(literal_prefix("/out/*/x.txt").unwrap(), Path::new("/out"));
        assert_eq!(literal_prefix("/*.txt").unwrap(), Path::new("/"));
        assert_eq!(
            literal_prefix("/out/report.txt").unwrap(),
            Path::new("/out/report.txt")
        );
        assert!(literal_prefix("out/*.txt").is_err());
    }

    #[test]
    fn strips_output_prefix() {
        assert_eq!(output_suffix("/out/a.txt", "/out").unwrap(), "a.txt");
        assert_eq!(
            output_suffix("/out/sub/deep/d.txt", "/out").unwrap(),
            "sub/deep/d.txt"
        );
        assert_eq!(
            output_suffix("/out/sub/c.txt", "/out/sub").unwrap(),
            "c.txt"
        );
        // A prefix must end on a component boundary and stay a strict ancestor.
        assert!(output_suffix("/output/a.txt", "/out").is_none());
        assert!(output_suffix("/out", "/out").is_none());
        assert!(output_suffix("/other/a.txt", "/out").is_none());
    }

    #[test]
    fn matches_declared_outputs() {
        let matcher = OutputMatcher::new(["/out/report.txt", "/out/*.csv"]).unwrap();
        assert!(matcher.is_match("/out/report.txt"));
        assert!(matcher.is_match("/out/report.csv"));
        assert!(!matcher.is_match("/out/sub/report.csv"));
        assert!(!matcher.is_match("/out/other.txt"));
    }
}
