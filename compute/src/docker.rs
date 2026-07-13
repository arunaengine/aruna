use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use bollard::Docker;
use bollard::models::{
    ContainerCreateBody, ContainerInspectResponse, ContainerStateStatusEnum, HostConfig,
};
use bollard::query_parameters::{
    CreateContainerOptionsBuilder, CreateImageOptionsBuilder, InspectContainerOptions,
    LogsOptionsBuilder, RemoveContainerOptionsBuilder, StartContainerOptions,
    StopContainerOptionsBuilder,
};
use futures_util::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::backend::{BackendError, ExecutorBackend, ExecutorKind};
use crate::logs::{BoundedTail, LogSink, LogStream, LogTails};
use crate::spec::{AttemptRef, LogLimits, TaskSpec};
use crate::status::{AttemptPhase, AttemptStatus, CancelEvidence, ReconcileOutcome};

/// Label carrying the effective walltime ceiling in milliseconds so `wait` can
/// enforce it against the daemon-reported start time.
const WALLTIME_LABEL: &str = "aruna.io/max-walltime-ms";

/// Security and default-limit configuration for the Docker backend. Per the
/// ceiling contract a request without a limit is filled from these defaults;
/// an explicit `None` is an operator's deliberate opt-out.
#[derive(Clone, Debug)]
pub struct DockerConfig {
    /// Graceful stop timeout before SIGKILL, in seconds.
    pub stop_grace_secs: i32,
    /// Retain the container after terminal evidence for debugging.
    pub keep_failed: bool,
    /// Memory ceiling applied when a request omits one (default 2 GiB).
    pub default_mem_bytes: Option<i64>,
    /// nano-CPU ceiling applied when a request omits one (default 2 cores).
    pub default_nano_cpus: Option<i64>,
    /// Walltime ceiling applied when a request omits one (default 24 h);
    /// enforced by `wait`, which stops the container past the deadline.
    pub default_max_walltime: Option<Duration>,
    pub pids_limit: i64,
    pub drop_all_caps: bool,
    pub no_new_privileges: bool,
    /// Container network mode; `None` keeps the daemon default (egress allowed).
    pub network_mode: Option<String>,
    /// Non-root user (`uid[:gid]`); `None` lets the image decide.
    pub user: Option<String>,
}

impl Default for DockerConfig {
    fn default() -> Self {
        Self {
            stop_grace_secs: 10,
            keep_failed: false,
            default_mem_bytes: Some(2 * 1024 * 1024 * 1024),
            default_nano_cpus: Some(2_000_000_000),
            default_max_walltime: Some(Duration::from_secs(24 * 60 * 60)),
            pids_limit: 2048,
            drop_all_caps: true,
            no_new_privileges: true,
            network_mode: None,
            user: None,
        }
    }
}

/// Local Docker/OCI executor backend over bollard.
pub struct DockerBackend {
    docker: Docker,
    config: DockerConfig,
}

impl DockerBackend {
    /// Connect using the environment's default transport (unix socket / npipe / DOCKER_HOST).
    pub fn connect() -> Result<Self, BackendError> {
        Self::with_config(DockerConfig::default())
    }

    pub fn with_config(config: DockerConfig) -> Result<Self, BackendError> {
        let docker = Docker::connect_with_defaults().map_err(|e| classify(&e))?;
        Ok(Self { docker, config })
    }

    pub fn from_parts(docker: Docker, config: DockerConfig) -> Self {
        Self { docker, config }
    }

    pub fn config(&self) -> &DockerConfig {
        &self.config
    }

    /// Raw inspect by attempt name (backend-specific; used for resource assertions).
    pub async fn inspect(
        &self,
        attempt: &AttemptRef,
    ) -> Result<ContainerInspectResponse, BackendError> {
        self.docker
            .inspect_container(&attempt.external_name(), None::<InspectContainerOptions>)
            .await
            .map_err(|e| classify(&e))
    }

    async fn ensure_image(&self, image: &str) -> Result<(), BackendError> {
        match self.docker.inspect_image(image).await {
            Ok(_) => Ok(()),
            Err(e) => match classify(&e) {
                BackendError::NotFound(_) => self.pull_image(image).await,
                other => Err(other),
            },
        }
    }

    async fn pull_image(&self, image: &str) -> Result<(), BackendError> {
        let (from_image, tag) = split_image_ref(image);
        let mut builder = CreateImageOptionsBuilder::new().from_image(&from_image);
        if let Some(tag) = &tag {
            builder = builder.tag(tag);
        }
        let mut stream = self.docker.create_image(Some(builder.build()), None, None);
        while let Some(item) = stream.next().await {
            match item {
                Ok(info) => {
                    if let Some(err) = info.error {
                        return Err(classify_pull_error(&err));
                    }
                }
                Err(e) => return Err(classify_pull(&e)),
            }
        }
        Ok(())
    }

    /// Stop tolerant of already-stopped (304) and already-gone (404).
    async fn stop_by_name(&self, name: &str) -> Result<(), BackendError> {
        let opts = StopContainerOptionsBuilder::new()
            .t(self.config.stop_grace_secs)
            .build();
        match self.docker.stop_container(name, Some(opts)).await {
            Ok(()) => Ok(()),
            Err(e) => match classify(&e) {
                BackendError::NotFound(_) | BackendError::Conflict(_) => Ok(()),
                other => Err(other),
            },
        }
    }

    async fn start_by_name(&self, name: &str) -> Result<(), BackendError> {
        match self
            .docker
            .start_container(name, None::<StartContainerOptions>)
            .await
        {
            Ok(()) => Ok(()),
            // 304 Not Modified: already running (submit race). Treat as success.
            Err(e) => match classify(&e) {
                BackendError::Conflict(_) => Ok(()),
                other => Err(other),
            },
        }
    }
}

fn build_config(config: &DockerConfig, spec: &TaskSpec) -> ContainerCreateBody {
    let env: Vec<String> = spec
        .effective_env()
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect();
    let mut labels: HashMap<String, String> = spec.attempt.labels().into_iter().collect();
    if let Some(walltime) = spec.resources.max_walltime.or(config.default_max_walltime) {
        labels.insert(WALLTIME_LABEL.to_string(), walltime.as_millis().to_string());
    }

    let memory = spec
        .resources
        .ram_bytes
        .map(|b| b as i64)
        .or(config.default_mem_bytes);
    let nano_cpus = spec
        .resources
        .cpu_cores
        .map(|c| c as i64 * 1_000_000_000)
        .or(config.default_nano_cpus);
    // Forwarded verbatim: a daemon whose storage driver cannot enforce a disk
    // quota rejects the create, which beats silently dropping the ceiling.
    let storage_opt = spec
        .resources
        .disk_bytes
        .map(|bytes| HashMap::from([("size".to_string(), bytes.to_string())]));

    let host_config = HostConfig {
        memory,
        memory_swap: memory,
        nano_cpus,
        storage_opt,
        pids_limit: Some(config.pids_limit),
        cap_drop: config.drop_all_caps.then(|| vec!["ALL".to_string()]),
        security_opt: config
            .no_new_privileges
            .then(|| vec!["no-new-privileges".to_string()]),
        network_mode: config.network_mode.clone(),
        auto_remove: Some(false),
        ..Default::default()
    };

    ContainerCreateBody {
        image: Some(spec.image.clone()),
        entrypoint: spec.entrypoint.clone(),
        cmd: (!spec.command.is_empty()).then(|| spec.command.clone()),
        env: Some(env),
        working_dir: spec.workdir.clone(),
        user: config.user.clone(),
        labels: Some(labels),
        host_config: Some(host_config),
        ..Default::default()
    }
}

#[async_trait]
impl ExecutorBackend for DockerBackend {
    fn kind(&self) -> ExecutorKind {
        ExecutorKind::Docker
    }

    async fn health(&self) -> Result<(), BackendError> {
        self.docker
            .ping()
            .await
            .map(|_| ())
            .map_err(|e| classify(&e))
    }

    async fn submit(&self, spec: &TaskSpec) -> Result<AttemptStatus, BackendError> {
        spec.attempt.validate().map_err(BackendError::InvalidSpec)?;
        // Never accept a resource request this backend cannot honor.
        if let Some(extension) = spec.resources.backend_extensions.keys().next() {
            return Err(BackendError::InvalidSpec(format!(
                "backend extension `{extension}` is not supported by the docker backend"
            )));
        }
        let name = spec.attempt.external_name();
        self.ensure_image(&spec.image).await?;

        let create_opts = CreateContainerOptionsBuilder::new().name(&name).build();
        match self
            .docker
            .create_container(Some(create_opts), build_config(&self.config, spec))
            .await
        {
            Ok(_) => {
                self.start_by_name(&name).await?;
                self.status(&spec.attempt).await
            }
            // Name collision: adopt the existing attempt, never start a second run.
            Err(e) => match classify(&e) {
                BackendError::Conflict(_) => {
                    let inspect = self.inspect(&spec.attempt).await?;
                    if !labels_match(&inspect, &spec.attempt) {
                        return Err(BackendError::Conflict(format!(
                            "container `{name}` exists but is not this attempt"
                        )));
                    }
                    let status = inspect_to_status(inspect);
                    // Complete a partial submit in place, but only for a container
                    // that provably never ran: `docker start` on an exited
                    // container would re-run it.
                    if is_fresh_created(&status) {
                        self.start_by_name(&name).await?;
                        return self.status(&spec.attempt).await;
                    }
                    Ok(status)
                }
                other => Err(other),
            },
        }
    }

    async fn status(&self, attempt: &AttemptRef) -> Result<AttemptStatus, BackendError> {
        Ok(inspect_to_status(self.inspect(attempt).await?))
    }

    async fn wait(
        &self,
        attempt: &AttemptRef,
        cancel: &CancellationToken,
    ) -> Result<AttemptStatus, BackendError> {
        // The daemon's wait endpoint (condition "not-running") answers instantly
        // for a created-but-never-started container, which would surface a
        // non-terminal status and break the wait contract. Poll inspect to
        // terminal evidence or the cancel token, like the trait default.
        loop {
            let inspect = self.inspect(attempt).await?;
            let deadline_ms = walltime_deadline_ms(&inspect);
            let status = inspect_to_status(inspect);
            if status.is_terminal() {
                return Ok(status);
            }
            // Enforce the walltime ceiling recorded at submit.
            if let Some(deadline_ms) = deadline_ms
                && now_ms() >= deadline_ms
            {
                self.stop_by_name(&attempt.external_name()).await?;
                let stopped = self.status(attempt).await?;
                return Ok(AttemptStatus {
                    phase: AttemptPhase::Failed {
                        reason: "max walltime exceeded".to_string(),
                    },
                    ..stopped
                });
            }
            tokio::select! {
                _ = cancel.cancelled() => return self.status(attempt).await,
                _ = tokio::time::sleep(Duration::from_millis(500)) => {}
            }
        }
    }

    async fn cancel(&self, attempt: &AttemptRef) -> Result<CancelEvidence, BackendError> {
        let name = attempt.external_name();
        let opts = StopContainerOptionsBuilder::new()
            .t(self.config.stop_grace_secs)
            .build();
        match self.docker.stop_container(&name, Some(opts)).await {
            Ok(()) => {}
            Err(e) => match classify(&e) {
                BackendError::NotFound(_) => return Ok(CancelEvidence::AlreadyGone),
                // 304: already stopped. Fall through to gather evidence.
                BackendError::Conflict(_) => {}
                other => return Err(other),
            },
        }
        let status = self.status(attempt).await?;
        if status.is_terminal() {
            Ok(CancelEvidence::Stopped(AttemptStatus {
                phase: AttemptPhase::Cancelled,
                ..status
            }))
        } else {
            Ok(CancelEvidence::Requested)
        }
    }

    async fn fetch_logs(
        &self,
        attempt: &AttemptRef,
        limits: &LogLimits,
        sink: &dyn LogSink,
    ) -> Result<LogTails, BackendError> {
        let name = attempt.external_name();
        let opts = LogsOptionsBuilder::new()
            .stdout(true)
            .stderr(true)
            .follow(false)
            .build();
        let mut stream = self.docker.logs(&name, Some(opts));
        let mut stdout = BoundedTail::new(limits.max_bytes_per_stream);
        let mut stderr = BoundedTail::new(limits.max_bytes_per_stream);
        while let Some(item) = stream.next().await {
            use bollard::container::LogOutput;
            match item.map_err(|e| classify(&e))? {
                LogOutput::StdOut { message } | LogOutput::Console { message } => {
                    sink.write(LogStream::Stdout, &message);
                    stdout.push(&message);
                }
                LogOutput::StdErr { message } => {
                    sink.write(LogStream::Stderr, &message);
                    stderr.push(&message);
                }
                LogOutput::StdIn { .. } => {}
            }
        }
        Ok(LogTails {
            stdout_total: stdout.total(),
            stderr_total: stderr.total(),
            stdout_truncated: stdout.truncated(),
            stderr_truncated: stderr.truncated(),
            stdout: stdout.into_bytes(),
            stderr: stderr.into_bytes(),
        })
    }

    async fn reconcile(&self, attempt: &AttemptRef) -> ReconcileOutcome {
        match self.inspect(attempt).await {
            // A same-named container without this attempt's labels is foreign
            // (another instance or an operator): not adoptable evidence.
            Ok(inspect) if !labels_match(&inspect, attempt) => {
                ReconcileOutcome::Unavailable(BackendError::Conflict(format!(
                    "container `{}` exists but is not this attempt",
                    attempt.external_name()
                )))
            }
            Ok(inspect) => ReconcileOutcome::Found(inspect_to_status(inspect)),
            Err(BackendError::NotFound(_)) => ReconcileOutcome::NotFound,
            Err(other) => ReconcileOutcome::Unavailable(other),
        }
    }

    async fn cleanup(&self, attempt: &AttemptRef) -> Result<(), BackendError> {
        if self.config.keep_failed {
            return Ok(());
        }
        let name = attempt.external_name();
        let inspect = match self.inspect(attempt).await {
            Ok(inspect) => inspect,
            Err(BackendError::NotFound(_)) => return Ok(()),
            Err(other) => return Err(other),
        };
        if !labels_match(&inspect, attempt) {
            return Err(BackendError::Conflict(format!(
                "refusing to remove foreign container `{name}`"
            )));
        }
        let opts = RemoveContainerOptionsBuilder::new().force(true).build();
        match self.docker.remove_container(&name, Some(opts)).await {
            Ok(()) => Ok(()),
            Err(e) => match classify(&e) {
                BackendError::NotFound(_) => Ok(()),
                other => Err(other),
            },
        }
    }
}

/// Epoch-ms walltime deadline of a started container, from the ceiling label
/// written at submit and the daemon-reported start time. `None` while the
/// container has not started, or when the operator opted out of a ceiling.
fn walltime_deadline_ms(inspect: &ContainerInspectResponse) -> Option<u64> {
    let walltime_ms: u64 = inspect
        .config
        .as_ref()?
        .labels
        .as_ref()?
        .get(WALLTIME_LABEL)?
        .parse()
        .ok()?;
    let started_ms = inspect
        .state
        .as_ref()?
        .started_at
        .as_deref()
        .and_then(parse_rfc3339_ms)?;
    Some(started_ms.saturating_add(walltime_ms))
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

/// The container carries this attempt's `aruna.io/*` labels; matching by the
/// deterministic name alone would adopt (or remove) a same-named container
/// created by another instance.
fn labels_match(inspect: &ContainerInspectResponse, attempt: &AttemptRef) -> bool {
    let Some(labels) = inspect
        .config
        .as_ref()
        .and_then(|config| config.labels.as_ref())
    else {
        return false;
    };
    attempt
        .labels()
        .iter()
        .all(|(key, value)| labels.get(key) == Some(value))
}

/// A container created by a partial submit that never started a run: only such a
/// fresh attempt may be started when adopting a name collision.
fn is_fresh_created(status: &AttemptStatus) -> bool {
    matches!(status.phase, AttemptPhase::Submitted)
        && status.started_at_ms.is_none()
        && status.finished_at_ms.is_none()
}

fn inspect_to_status(inspect: ContainerInspectResponse) -> AttemptStatus {
    let backend_ref = inspect.id.clone().unwrap_or_default();
    let state = inspect.state.unwrap_or_default();
    let started_at_ms = state.started_at.as_deref().and_then(parse_rfc3339_ms);
    let finished_at_ms = state.finished_at.as_deref().and_then(parse_rfc3339_ms);
    let exit_code = state.exit_code.unwrap_or_default() as i32;

    let phase = match state.status.unwrap_or(ContainerStateStatusEnum::EMPTY) {
        ContainerStateStatusEnum::RUNNING
        | ContainerStateStatusEnum::PAUSED
        | ContainerStateStatusEnum::RESTARTING
        | ContainerStateStatusEnum::REMOVING => AttemptPhase::Running,
        ContainerStateStatusEnum::CREATED | ContainerStateStatusEnum::EMPTY => {
            AttemptPhase::Submitted
        }
        ContainerStateStatusEnum::EXITED | ContainerStateStatusEnum::DEAD => {
            AttemptPhase::Exited { code: exit_code }
        }
    };

    AttemptStatus {
        phase,
        backend_ref,
        started_at_ms,
        finished_at_ms,
    }
}

fn classify(err: &bollard::errors::Error) -> BackendError {
    use bollard::errors::Error;
    match err {
        Error::DockerResponseServerError {
            status_code,
            message,
        } => match status_code {
            304 | 409 => BackendError::Conflict(message.clone()),
            400 => BackendError::InvalidSpec(message.clone()),
            401 | 403 => BackendError::ImageUnauthorized(message.clone()),
            404 => BackendError::NotFound(message.clone()),
            500..=599 => BackendError::Unavailable(message.clone()),
            other => BackendError::Api(format!("status {other}: {message}")),
        },
        Error::IOError { .. } | Error::DockerStreamError { .. } => {
            BackendError::Unavailable(err.to_string())
        }
        _ => BackendError::Api(err.to_string()),
    }
}

/// Pull-scoped HTTP classification. Registries answer a missing or unreadable
/// repository with a 404 ("pull access denied", "repository does not exist"),
/// which must become a permanent Image* error, never a retryable `NotFound`.
fn classify_pull(err: &bollard::errors::Error) -> BackendError {
    use bollard::errors::Error;
    match err {
        Error::DockerResponseServerError {
            status_code,
            message,
        } => match status_code {
            400 => BackendError::InvalidSpec(message.clone()),
            401 | 403 => BackendError::ImageUnauthorized(message.clone()),
            404 => BackendError::ImageNotFound(message.clone()),
            _ => classify_pull_error(message),
        },
        _ => classify(err),
    }
}

fn classify_pull_error(message: &str) -> BackendError {
    let lower = message.to_lowercase();
    if lower.contains("not found")
        || lower.contains("manifest unknown")
        || lower.contains("repository does not exist")
        || lower.contains("no such")
    {
        BackendError::ImageNotFound(message.to_string())
    } else if lower.contains("unauthorized")
        || lower.contains("denied")
        || lower.contains("authentication")
        || lower.contains("forbidden")
    {
        BackendError::ImageUnauthorized(message.to_string())
    } else {
        BackendError::Unavailable(message.to_string())
    }
}

/// Split an image reference into (repo, tag-or-digest) for the pull API.
fn split_image_ref(image: &str) -> (String, Option<String>) {
    if let Some((repo, digest)) = image.split_once('@') {
        return (repo.to_string(), Some(digest.to_string()));
    }
    let last_slash = image.rfind('/');
    if let Some(colon) = image.rfind(':')
        && last_slash.is_none_or(|slash| colon > slash)
    {
        return (
            image[..colon].to_string(),
            Some(image[colon + 1..].to_string()),
        );
    }
    (image.to_string(), Some("latest".to_string()))
}

/// Parse a Docker RFC3339 UTC timestamp to epoch milliseconds. Returns `None`
/// for the daemon's zero value and anything unparseable.
fn parse_rfc3339_ms(s: &str) -> Option<u64> {
    if s.len() < 20 {
        return None;
    }
    let year: i64 = s.get(0..4)?.parse().ok()?;
    let month: i64 = s.get(5..7)?.parse().ok()?;
    let day: i64 = s.get(8..10)?.parse().ok()?;
    let hour: i64 = s.get(11..13)?.parse().ok()?;
    let minute: i64 = s.get(14..16)?.parse().ok()?;
    let second: i64 = s.get(17..19)?.parse().ok()?;
    if year <= 1 {
        return None;
    }
    let mut millis: i64 = 0;
    if let Some(dot) = s.find('.') {
        let digits: String = s[dot + 1..]
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .take(3)
            .collect();
        millis = digits.parse().unwrap_or(0);
        for _ in digits.len()..3 {
            millis *= 10;
        }
    }
    let days = days_from_civil(year, month, day);
    let total = (days * 86_400 + hour * 3_600 + minute * 60 + second) * 1_000 + millis;
    u64::try_from(total).ok()
}

/// Days since 1970-01-01 for a proleptic Gregorian date (Howard Hinnant).
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn image_split() {
        // Tag, digest, registry-port, and bare-name references all parse.
        assert_eq!(
            split_image_ref("alpine"),
            ("alpine".into(), Some("latest".into()))
        );
        assert_eq!(
            split_image_ref("alpine:3.20"),
            ("alpine".into(), Some("3.20".into()))
        );
        assert_eq!(
            split_image_ref("reg:5000/repo/img:tag"),
            ("reg:5000/repo/img".into(), Some("tag".into()))
        );
        assert_eq!(
            split_image_ref("localhost:5000/img"),
            ("localhost:5000/img".into(), Some("latest".into()))
        );
        assert_eq!(
            split_image_ref("repo@sha256:abc"),
            ("repo".into(), Some("sha256:abc".into()))
        );
    }

    #[test]
    fn label_gate() {
        use bollard::models::ContainerConfig;
        let attempt = AttemptRef::new("j1", 0);
        let mut inspect = ContainerInspectResponse {
            config: Some(ContainerConfig {
                labels: Some(attempt.labels().into_iter().collect()),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(labels_match(&inspect, &attempt));
        // Another job, another attempt number, or an unlabeled container never matches.
        assert!(!labels_match(&inspect, &AttemptRef::new("j2", 0)));
        assert!(!labels_match(&inspect, &AttemptRef::new("j1", 1)));
        inspect.config.as_mut().unwrap().labels = None;
        assert!(!labels_match(&inspect, &attempt));
    }

    #[test]
    fn default_ceilings() {
        // A request without limits is filled from config defaults, never unlimited.
        let config = DockerConfig::default();
        let spec = TaskSpec::new(AttemptRef::new("j1", 0), "alpine");
        let body = build_config(&config, &spec);
        let host = body.host_config.unwrap();
        assert!(host.memory.is_some());
        assert_eq!(host.memory, config.default_mem_bytes);
        assert!(host.nano_cpus.is_some());
        assert_eq!(host.nano_cpus, config.default_nano_cpus);
        assert!(host.storage_opt.is_none());
        let labels = body.labels.unwrap();
        assert_eq!(
            labels.get(WALLTIME_LABEL).unwrap(),
            &(24 * 60 * 60 * 1000).to_string()
        );
    }

    #[test]
    fn request_ceilings() {
        // Explicit requests win over defaults; disk becomes a storage quota.
        let mut spec = TaskSpec::new(AttemptRef::new("j1", 0), "alpine");
        spec.resources.ram_bytes = Some(64 * 1024 * 1024);
        spec.resources.cpu_cores = Some(1);
        spec.resources.disk_bytes = Some(1 << 30);
        spec.resources.max_walltime = Some(Duration::from_secs(600));
        let body = build_config(&DockerConfig::default(), &spec);
        let host = body.host_config.unwrap();
        assert_eq!(host.memory, Some(64 * 1024 * 1024));
        assert_eq!(host.nano_cpus, Some(1_000_000_000));
        assert_eq!(
            host.storage_opt.unwrap().get("size").unwrap(),
            &(1u64 << 30).to_string()
        );
        assert_eq!(body.labels.unwrap().get(WALLTIME_LABEL).unwrap(), "600000");
    }

    #[test]
    fn walltime_deadline() {
        use bollard::models::{ContainerConfig, ContainerState};
        let inspect = ContainerInspectResponse {
            config: Some(ContainerConfig {
                labels: Some(HashMap::from([(
                    WALLTIME_LABEL.to_string(),
                    "1000".to_string(),
                )])),
                ..Default::default()
            }),
            state: Some(ContainerState {
                started_at: Some("2024-01-01T00:00:00Z".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            walltime_deadline_ms(&inspect),
            Some(1_704_067_200_000 + 1000)
        );
        // Not started yet (daemon zero value) or no ceiling label: no deadline.
        let mut unstarted = inspect.clone();
        unstarted.state.as_mut().unwrap().started_at = Some("0001-01-01T00:00:00Z".to_string());
        assert_eq!(walltime_deadline_ms(&unstarted), None);
        let mut unlimited = inspect;
        unlimited.config.as_mut().unwrap().labels = Some(HashMap::new());
        assert_eq!(walltime_deadline_ms(&unlimited), None);
    }

    #[test]
    fn freshness_gate() {
        let fresh = AttemptStatus {
            phase: AttemptPhase::Submitted,
            backend_ref: "id".to_string(),
            started_at_ms: None,
            finished_at_ms: None,
        };
        assert!(is_fresh_created(&fresh));
        // Anything that ever ran (or whose state is unreadable but timestamped)
        // must be adopted as evidence, never started again.
        assert!(!is_fresh_created(&AttemptStatus {
            started_at_ms: Some(1),
            ..fresh.clone()
        }));
        assert!(!is_fresh_created(&AttemptStatus {
            finished_at_ms: Some(2),
            ..fresh.clone()
        }));
        assert!(!is_fresh_created(&AttemptStatus {
            phase: AttemptPhase::Exited { code: 0 },
            ..fresh.clone()
        }));
        assert!(!is_fresh_created(&AttemptStatus {
            phase: AttemptPhase::Running,
            ..fresh
        }));
    }

    #[test]
    fn pull_classification() {
        use bollard::errors::Error;
        // A registry 404 ("pull access denied", "repository does not exist") is a
        // permanent image error, never a retryable attempt-NotFound.
        let denied = Error::DockerResponseServerError {
            status_code: 404,
            message: "pull access denied for private/img".to_string(),
        };
        let classified = classify_pull(&denied);
        assert!(matches!(classified, BackendError::ImageNotFound(_)));
        assert!(!classified.retryable());

        let unauthorized = Error::DockerResponseServerError {
            status_code: 403,
            message: "forbidden".to_string(),
        };
        let classified = classify_pull(&unauthorized);
        assert!(matches!(classified, BackendError::ImageUnauthorized(_)));
        assert!(!classified.retryable());

        // A daemon 500 wrapping a registry auth failure is still permanent.
        let wrapped = Error::DockerResponseServerError {
            status_code: 500,
            message: "unauthorized: authentication required".to_string(),
        };
        assert!(!classify_pull(&wrapped).retryable());

        // Genuinely transient faults stay retryable.
        let flaky = Error::DockerResponseServerError {
            status_code: 500,
            message: "registry timeout".to_string(),
        };
        assert!(classify_pull(&flaky).retryable());
        let io = Error::IOError {
            err: std::io::Error::other("socket closed"),
        };
        assert!(classify_pull(&io).retryable());
    }

    #[test]
    fn rfc3339_epoch() {
        // Epoch, sub-second, the daemon zero value, and junk.
        assert_eq!(parse_rfc3339_ms("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(
            parse_rfc3339_ms("2024-01-01T00:00:00Z"),
            Some(1_704_067_200_000)
        );
        assert_eq!(
            parse_rfc3339_ms("2024-01-01T00:00:00.5Z"),
            Some(1_704_067_200_500)
        );
        assert_eq!(parse_rfc3339_ms("0001-01-01T00:00:00Z"), None);
        assert_eq!(parse_rfc3339_ms("garbage"), None);
    }
}
