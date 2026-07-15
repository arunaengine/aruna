use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

#[cfg(test)]
use std::io::Cursor;

use async_trait::async_trait;
use bollard::models::{
    ContainerCreateBody, ContainerInspectResponse, ContainerStateStatusEnum, HostConfig,
};
use bollard::query_parameters::{
    ContainerArchiveInfoOptionsBuilder, CreateContainerOptionsBuilder, CreateImageOptionsBuilder,
    DownloadFromContainerOptionsBuilder, InspectContainerOptions, LogsOptionsBuilder,
    RemoveContainerOptionsBuilder, StartContainerOptions, StopContainerOptionsBuilder,
    UploadToContainerOptionsBuilder,
};
use bollard::{Docker, body_full};
use bytes::{Buf, Bytes};
use futures_util::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::backend::{BackendError, ExecutorBackend, ExecutorKind};
use crate::logs::{BoundedTail, LogSink, LogStream, LogTails};
use crate::spec::{AttemptRef, LogLimits, MAX_TRANSFER_BYTES, TaskInput, TaskSpec};
use crate::status::{AttemptPhase, AttemptStatus, CancelEvidence, ReconcileOutcome};

/// Label carrying the effective walltime ceiling in milliseconds so `wait` can
/// enforce it against the daemon-reported start time.
const WALLTIME_LABEL: &str = "aruna-engine.org/max-walltime-ms";
/// Docker encodes directory type with Go's `os.ModeDir` bit.
const DIRECTORY_MODE: u32 = 1 << 31;

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
    /// Writable-layer ceiling applied when a request omits one.
    pub default_disk_bytes: Option<u64>,
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
            default_disk_bytes: None,
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

enum RemoveOutcome {
    Removed,
    Gone,
    Foreign,
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

    async fn inspect_matching_attempt(
        &self,
        attempt: &AttemptRef,
    ) -> Result<ContainerInspectResponse, BackendError> {
        let inspect = self.inspect(attempt).await?;
        if !labels_match(&inspect, attempt) {
            return Err(BackendError::Conflict(format!(
                "container `{}` exists but is not this attempt",
                attempt.external_name()
            )));
        }
        Ok(inspect)
    }

    async fn ensure_image(
        &self,
        image: &str,
        cancel: &CancellationToken,
    ) -> Result<(), BackendError> {
        check_cancel(cancel)?;
        let inspected = tokio::select! {
            _ = cancel.cancelled() => return Err(BackendError::Cancelled),
            result = self.docker.inspect_image(image) => result,
        };
        check_cancel(cancel)?;
        match inspected {
            Ok(_) => Ok(()),
            Err(e) => match classify(&e) {
                BackendError::NotFound(_) => self.pull_image(image, cancel).await,
                other => Err(other),
            },
        }
    }

    async fn pull_image(
        &self,
        image: &str,
        cancel: &CancellationToken,
    ) -> Result<(), BackendError> {
        let (from_image, tag) = split_image_ref(image);
        let mut builder = CreateImageOptionsBuilder::new().from_image(&from_image);
        if let Some(tag) = &tag {
            builder = builder.tag(tag);
        }
        let mut stream = self.docker.create_image(Some(builder.build()), None, None);
        loop {
            check_cancel(cancel)?;
            let item = tokio::select! {
                _ = cancel.cancelled() => return Err(BackendError::Cancelled),
                item = stream.next() => item,
            };
            let Some(item) = item else {
                break;
            };
            match item {
                Ok(info) => {
                    if let Some(err) = info.error_detail.and_then(|detail| detail.message) {
                        return Err(classify_pull_error(&err));
                    }
                }
                Err(e) => return Err(classify_pull(&e)),
            }
        }
        check_cancel(cancel)
    }

    async fn force_remove(&self, attempt: &AttemptRef) -> Result<RemoveOutcome, BackendError> {
        let inspect = match self.inspect(attempt).await {
            Ok(inspect) => inspect,
            Err(BackendError::NotFound(_)) => return Ok(RemoveOutcome::Gone),
            Err(other) => return Err(other),
        };
        if !labels_match(&inspect, attempt) {
            return Ok(RemoveOutcome::Foreign);
        }
        let container_id = inspect.id.as_deref().ok_or_else(|| {
            BackendError::Api("Docker inspect response omitted the container ID".to_string())
        })?;
        let opts = RemoveContainerOptionsBuilder::new()
            .v(true)
            .force(true)
            .build();
        match self.docker.remove_container(container_id, Some(opts)).await {
            Ok(()) => Ok(RemoveOutcome::Removed),
            Err(e) => match classify(&e) {
                BackendError::NotFound(_) => Ok(RemoveOutcome::Gone),
                other => Err(other),
            },
        }
    }

    async fn cancel_submission(
        &self,
        attempt: &AttemptRef,
        cancel: &CancellationToken,
    ) -> Result<(), BackendError> {
        if !cancel.is_cancelled() {
            return Ok(());
        }
        match self.status(attempt).await {
            Ok(status) if status.is_terminal() => return Ok(()),
            Ok(_) => {}
            Err(BackendError::NotFound(_)) => return Err(BackendError::Cancelled),
            Err(other) => return Err(other),
        }
        match self.force_remove(attempt).await? {
            RemoveOutcome::Removed | RemoveOutcome::Gone => Err(BackendError::Cancelled),
            RemoveOutcome::Foreign => Err(BackendError::Conflict(format!(
                "refusing to remove foreign container `{}`",
                attempt.external_name()
            ))),
        }
    }

    /// Stop a matching attempt by ID, tolerating 304 or removal after inspection.
    async fn stop_attempt(&self, attempt: &AttemptRef) -> Result<bool, BackendError> {
        let inspect = self.inspect_matching_attempt(attempt).await?;
        let container_id = inspect.id.as_deref().ok_or_else(|| {
            BackendError::Api("Docker inspect response omitted the container ID".to_string())
        })?;
        let opts = StopContainerOptionsBuilder::new()
            .t(self.config.stop_grace_secs)
            .build();
        match self.docker.stop_container(container_id, Some(opts)).await {
            Ok(()) => Ok(true),
            Err(e) => match classify(&e) {
                BackendError::NotFound(_) | BackendError::Conflict(_) => Ok(false),
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

    async fn upload_archive(&self, container: &str, archive: Vec<u8>) -> Result<(), BackendError> {
        let options = UploadToContainerOptionsBuilder::new()
            .path("/")
            .no_overwrite_dir_non_dir("true")
            .build();
        self.docker
            .upload_to_container(container, Some(options), body_full(Bytes::from(archive)))
            .await
            .map_err(|error| classify_archive(&error))
    }

    async fn prepare_created(
        &self,
        attempt: &AttemptRef,
        plan: Option<&ArchivePlan<'_>>,
        inspect: ContainerInspectResponse,
        cancel: &CancellationToken,
    ) -> Result<AttemptStatus, BackendError> {
        self.cancel_submission(attempt, cancel).await?;
        let status = inspect_to_status(inspect.clone());
        if !is_fresh_created(&status) {
            return Ok(status);
        }
        let container_id = inspect.id.as_deref().ok_or_else(|| {
            BackendError::Api("Docker inspect response omitted the container ID".to_string())
        })?;
        if let Some(plan) = plan {
            let directories = self.missing_dirs(container_id, &plan.directories).await;
            self.cancel_submission(attempt, cancel).await?;
            let directories = directories?;
            if !plan.inputs.is_empty() || !directories.is_empty() {
                let archive = build_archive(plan, &directories);
                self.cancel_submission(attempt, cancel).await?;
                let uploaded = self.upload_archive(container_id, archive?).await;
                self.cancel_submission(attempt, cancel).await?;
                uploaded?;
            }
        }
        self.cancel_submission(attempt, cancel).await?;
        let started = self.start_by_name(container_id).await;
        self.cancel_submission(attempt, cancel).await?;
        started?;
        let status = self.status(attempt).await;
        self.cancel_submission(attempt, cancel).await?;
        status
    }

    async fn missing_dirs(
        &self,
        container: &str,
        directories: &BTreeMap<PathBuf, u32>,
    ) -> Result<BTreeMap<PathBuf, u32>, BackendError> {
        let mut missing = BTreeMap::new();
        for (path, mode) in directories {
            let absolute = format!("/{}", path.display());
            let options = ContainerArchiveInfoOptionsBuilder::new()
                .path(&absolute)
                .build();
            match self
                .docker
                .get_container_archive_info(container, Some(options))
                .await
            {
                Ok(stat) if stat.file_mode & DIRECTORY_MODE != 0 => {}
                Ok(_) => {
                    return Err(BackendError::InvalidSpec(format!(
                        "container parent path `{absolute}` is not a directory"
                    )));
                }
                Err(error) => match classify_archive(&error) {
                    BackendError::NotFound(_) => {
                        missing.insert(path.clone(), *mode);
                    }
                    other => return Err(other),
                },
            }
        }
        Ok(missing)
    }
}

fn container_path(path: &str) -> Result<PathBuf, BackendError> {
    let Some(relative) = path.strip_prefix('/') else {
        return Err(BackendError::InvalidSpec(format!(
            "container path `{path}` is not absolute"
        )));
    };
    if relative.is_empty()
        || relative
            .split('/')
            .any(|part| part.is_empty() || matches!(part, "." | "..") || part.contains('\0'))
    {
        return Err(BackendError::InvalidSpec(format!(
            "container path `{path}` is not a safe file path"
        )));
    }
    Ok(PathBuf::from(relative))
}

fn add_parents(path: &Path, mode: u32, directories: &mut BTreeMap<PathBuf, u32>) {
    let mut parent = path.parent();
    while let Some(path) = parent {
        if path.as_os_str().is_empty() {
            break;
        }
        directories
            .entry(path.to_path_buf())
            .and_modify(|current| *current = (*current).max(mode))
            .or_insert(mode);
        parent = path.parent();
    }
}

struct ArchivePlan<'a> {
    inputs: BTreeMap<PathBuf, &'a TaskInput>,
    directories: BTreeMap<PathBuf, u32>,
    input_bytes: usize,
}

impl<'a> ArchivePlan<'a> {
    fn new(spec: &'a TaskSpec) -> Result<Self, BackendError> {
        let mut inputs = BTreeMap::new();
        let mut outputs = BTreeSet::new();
        let mut files = BTreeSet::new();
        let mut input_bytes = 0usize;
        for input in &spec.inputs {
            let path = container_path(&input.path)?;
            if inputs.insert(path.clone(), input).is_some() || !files.insert(path) {
                return Err(BackendError::InvalidSpec(format!(
                    "duplicate input path `{}`",
                    input.path
                )));
            }
            input_bytes = input_bytes
                .checked_add(input.contents.len())
                .filter(|total| *total <= MAX_TRANSFER_BYTES)
                .ok_or_else(transfer_error)?;
        }
        for output in &spec.output_paths {
            let path = container_path(output)?;
            if !outputs.insert(path.clone()) || !files.insert(path) {
                return Err(BackendError::InvalidSpec(format!(
                    "duplicate or conflicting output path `{output}`"
                )));
            }
        }
        for path in &files {
            for parent in path.ancestors().skip(1) {
                if parent.as_os_str().is_empty() {
                    break;
                }
                if files.contains(parent) {
                    return Err(BackendError::InvalidSpec(format!(
                        "container file path `{}` is nested below another file",
                        path.display()
                    )));
                }
            }
        }

        let mut directories = BTreeMap::new();
        for path in inputs.keys() {
            add_parents(path, 0o755, &mut directories);
        }
        for path in &outputs {
            add_parents(path, 0o777, &mut directories);
        }

        Ok(Self {
            inputs,
            directories,
            input_bytes,
        })
    }
}

struct TransferBuffer {
    bytes: Vec<u8>,
    limit: usize,
}

impl Write for TransferBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.bytes.len().saturating_add(bytes.len()) > self.limit {
            return Err(io::Error::other("task file transfer exceeds memory limit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn transfer_error() -> BackendError {
    BackendError::InvalidSpec(format!(
        "task file transfer exceeds the {MAX_TRANSFER_BYTES}-byte limit"
    ))
}

fn build_archive(
    plan: &ArchivePlan<'_>,
    directories: &BTreeMap<PathBuf, u32>,
) -> Result<Vec<u8>, BackendError> {
    build_limited(plan, directories, MAX_TRANSFER_BYTES)
}

fn build_limited(
    plan: &ArchivePlan<'_>,
    directories: &BTreeMap<PathBuf, u32>,
    limit: usize,
) -> Result<Vec<u8>, BackendError> {
    let archive_limit = limit
        .checked_sub(plan.input_bytes)
        .ok_or_else(transfer_error)?;
    let buffer = TransferBuffer {
        bytes: Vec::new(),
        limit: archive_limit,
    };

    let mut builder = tar::Builder::new(buffer);
    for (path, mode) in directories {
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Directory);
        header.set_mode(*mode);
        header.set_uid(0);
        header.set_gid(0);
        header.set_mtime(0);
        header.set_size(0);
        builder
            .append_data(&mut header, path, std::io::empty())
            .map_err(|_| transfer_error())?;
    }
    for (path, input) in &plan.inputs {
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Regular);
        header.set_mode(0o444);
        header.set_uid(0);
        header.set_gid(0);
        header.set_mtime(0);
        header.set_size(input.contents.len() as u64);
        builder
            .append_data(&mut header, path, input.contents.as_slice())
            .map_err(|_| transfer_error())?;
    }
    builder
        .into_inner()
        .map(|buffer| buffer.bytes)
        .map_err(|_| transfer_error())
}

fn archive_api(error: impl std::fmt::Display) -> BackendError {
    BackendError::InvalidSpec(format!("invalid Docker output archive: {error}"))
}

fn output_spec(message: impl Into<String>) -> BackendError {
    BackendError::InvalidSpec(format!("invalid Docker output: {}", message.into()))
}

fn parse_output(
    archive: impl Read,
    archive_bytes: usize,
    expected: &Path,
) -> Result<Vec<u8>, BackendError> {
    parse_limited(archive, archive_bytes, expected, MAX_TRANSFER_BYTES)
}

fn parse_limited(
    archive: impl Read,
    archive_bytes: usize,
    expected: &Path,
    limit: usize,
) -> Result<Vec<u8>, BackendError> {
    if archive_bytes > limit {
        return Err(transfer_error());
    }
    let expected = expected
        .file_name()
        .ok_or_else(|| output_spec("path has no file name"))?;
    let mut archive = tar::Archive::new(archive);
    let mut entries = archive.entries().map_err(archive_api)?;
    let mut entry = entries
        .next()
        .ok_or_else(|| output_spec("archive is empty"))?
        .map_err(archive_api)?;
    let path = entry.path().map_err(archive_api)?;
    if path.is_absolute()
        || path
            .components()
            .any(|part| !matches!(part, std::path::Component::Normal(_)))
    {
        return Err(output_spec("archive entry path is unsafe"));
    }
    if path != Path::new(expected) {
        return Err(output_spec(format!(
            "archive contains unexpected entry `{}`",
            path.display()
        )));
    }
    if !entry.header().entry_type().is_file() || entry.link_name().map_err(archive_api)?.is_some() {
        return Err(output_spec("declared output is not a regular file"));
    }
    let size = usize::try_from(entry.size()).map_err(archive_api)?;
    if archive_bytes
        .checked_add(size)
        .is_none_or(|total| total > limit)
    {
        return Err(transfer_error());
    }
    let mut contents = vec![0; size];
    entry.read_exact(&mut contents).map_err(archive_api)?;
    drop(entry);
    if entries.next().transpose().map_err(archive_api)?.is_some() {
        return Err(output_spec("archive contains multiple entries"));
    }
    Ok(contents)
}

struct ArchiveChunks {
    chunks: VecDeque<Bytes>,
}

impl Read for ArchiveChunks {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        while self.chunks.front().is_some_and(Bytes::is_empty) {
            self.chunks.pop_front();
        }
        let Some(chunk) = self.chunks.front_mut() else {
            return Ok(0);
        };
        let count = buffer.len().min(chunk.len());
        buffer[..count].copy_from_slice(&chunk[..count]);
        chunk.advance(count);
        Ok(count)
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
        .or(config.default_disk_bytes)
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

    async fn submit(
        &self,
        spec: &TaskSpec,
        cancel: &CancellationToken,
    ) -> Result<AttemptStatus, BackendError> {
        spec.attempt.validate().map_err(BackendError::InvalidSpec)?;
        check_cancel(cancel)?;
        // Never accept a resource request this backend cannot honor.
        if let Some(extension) = spec.resources.backend_extensions.keys().next() {
            return Err(BackendError::InvalidSpec(format!(
                "backend extension `{extension}` is not supported by the docker backend"
            )));
        }
        let plan = (!spec.inputs.is_empty() || !spec.output_paths.is_empty())
            .then(|| ArchivePlan::new(spec))
            .transpose()?;
        let name = spec.attempt.external_name();
        self.ensure_image(&spec.image, cancel).await?;

        let create_opts = CreateContainerOptionsBuilder::new().name(&name).build();
        let created = self
            .docker
            .create_container(Some(create_opts), build_config(&self.config, spec))
            .await;
        self.cancel_submission(&spec.attempt, cancel).await?;
        match created {
            Ok(response) => {
                for warning in &response.warnings {
                    tracing::warn!(container = %name, warning = %warning, "Docker create warning");
                }
                let inspect = self.inspect_matching_attempt(&spec.attempt).await;
                self.cancel_submission(&spec.attempt, cancel).await?;
                self.prepare_created(&spec.attempt, plan.as_ref(), inspect?, cancel)
                    .await
            }
            // Name collision: adopt the existing attempt, never start a second run.
            Err(e) => match classify(&e) {
                BackendError::Conflict(_) => {
                    let inspect = self.inspect_matching_attempt(&spec.attempt).await;
                    self.cancel_submission(&spec.attempt, cancel).await?;
                    self.prepare_created(&spec.attempt, plan.as_ref(), inspect?, cancel)
                        .await
                }
                other => {
                    self.cancel_submission(&spec.attempt, cancel).await?;
                    Err(other)
                }
            },
        }
    }

    async fn status(&self, attempt: &AttemptRef) -> Result<AttemptStatus, BackendError> {
        attempt.validate().map_err(BackendError::InvalidSpec)?;
        Ok(inspect_to_status(
            self.inspect_matching_attempt(attempt).await?,
        ))
    }

    async fn wait(
        &self,
        attempt: &AttemptRef,
        cancel: &CancellationToken,
    ) -> Result<AttemptStatus, BackendError> {
        attempt.validate().map_err(BackendError::InvalidSpec)?;
        // The daemon's wait endpoint (condition "not-running") answers instantly
        // for a created-but-never-started container, which would surface a
        // non-terminal status and break the wait contract. Poll inspect to
        // terminal evidence or the cancel token, like the trait default.
        loop {
            let inspect = self.inspect_matching_attempt(attempt).await?;
            let deadline_ms = walltime_deadline_ms(&inspect);
            let status = inspect_to_status(inspect);
            if status.is_terminal() {
                return Ok(status);
            }
            // Enforce the walltime ceiling recorded at submit.
            if let Some(deadline_ms) = deadline_ms
                && now_ms() >= deadline_ms
            {
                let _ = self.stop_attempt(attempt).await?;
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
        attempt.validate().map_err(BackendError::InvalidSpec)?;
        // A container that had already exited on its own must keep its real exit evidence:
        // reporting Cancelled over it would fabricate a cancellation that never happened.
        let status = match self.status(attempt).await {
            Ok(status) => status,
            Err(BackendError::NotFound(_)) => return Ok(CancelEvidence::AlreadyGone),
            Err(other) => return Err(other),
        };
        if matches!(status.phase, AttemptPhase::Submitted) {
            return match self.force_remove(attempt).await? {
                RemoveOutcome::Removed | RemoveOutcome::Gone => Ok(CancelEvidence::AlreadyGone),
                RemoveOutcome::Foreign => Err(BackendError::Conflict(format!(
                    "refusing to remove foreign container `{}`",
                    attempt.external_name()
                ))),
            };
        }
        let was_running = matches!(status.phase, AttemptPhase::Running);
        let stop_effective = match self.stop_attempt(attempt).await {
            Ok(stop_effective) => stop_effective,
            Err(error) => {
                return match error {
                    BackendError::NotFound(_) => Ok(CancelEvidence::AlreadyGone),
                    other => Err(other),
                };
            }
        };
        let status = match self.status(attempt).await {
            Ok(status) => status,
            Err(BackendError::NotFound(_)) => return Ok(CancelEvidence::AlreadyGone),
            Err(other) => return Err(other),
        };
        if status.is_terminal() {
            if was_running && stop_effective {
                Ok(CancelEvidence::Stopped(AttemptStatus {
                    phase: AttemptPhase::Cancelled,
                    ..status
                }))
            } else {
                Ok(CancelEvidence::Stopped(status))
            }
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
        attempt.validate().map_err(BackendError::InvalidSpec)?;
        let inspect = self.inspect_matching_attempt(attempt).await?;
        let container_id = inspect.id.as_deref().ok_or_else(|| {
            BackendError::Api("Docker inspect response omitted the container ID".to_string())
        })?;
        let opts = LogsOptionsBuilder::new()
            .stdout(true)
            .stderr(true)
            .follow(false)
            .build();
        let mut stream = self.docker.logs(container_id, Some(opts));
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

    async fn fetch_output(
        &self,
        attempt: &AttemptRef,
        path: &str,
    ) -> Result<Vec<u8>, BackendError> {
        attempt.validate().map_err(BackendError::InvalidSpec)?;
        let output_path = container_path(path)?;
        let inspect = self.inspect_matching_attempt(attempt).await?;
        let status = inspect_to_status(inspect.clone());
        if !status.is_terminal() {
            return Err(output_spec("attempt is not terminal"));
        }
        let container_id = inspect.id.as_deref().ok_or_else(|| {
            BackendError::Api("Docker inspect response omitted the container ID".to_string())
        })?;
        let options = DownloadFromContainerOptionsBuilder::new()
            .path(path)
            .build();
        let mut stream = self
            .docker
            .download_from_container(container_id, Some(options));
        let mut archive = Vec::new();
        let mut archive_bytes = 0usize;
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(chunk) => {
                    archive_bytes = archive_bytes
                        .checked_add(chunk.len())
                        .filter(|total| *total <= MAX_TRANSFER_BYTES)
                        .ok_or_else(transfer_error)?;
                    archive.push(chunk);
                }
                Err(error) => {
                    let classified = classify_archive(&error);
                    if matches!(classified, BackendError::NotFound(_)) {
                        return match self.inspect_matching_attempt(attempt).await {
                            Ok(_) => Err(output_spec(format!("path `{path}` does not exist"))),
                            Err(error) => Err(error),
                        };
                    }
                    return Err(classified);
                }
            }
        }
        parse_output(
            ArchiveChunks {
                chunks: archive.into(),
            },
            archive_bytes,
            &output_path,
        )
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
        attempt.validate().map_err(BackendError::InvalidSpec)?;
        if self.config.keep_failed {
            return Ok(());
        }
        match self.force_remove(attempt).await? {
            RemoveOutcome::Removed | RemoveOutcome::Gone => Ok(()),
            RemoveOutcome::Foreign => Err(BackendError::Conflict(format!(
                "refusing to remove foreign container `{}`",
                attempt.external_name()
            ))),
        }
    }
}

fn check_cancel(cancel: &CancellationToken) -> Result<(), BackendError> {
    if cancel.is_cancelled() {
        Err(BackendError::Cancelled)
    } else {
        Ok(())
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

/// The container carries this attempt's `aruna-engine.org/*` labels; matching by the
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
    // An absent exit code is NOT a zero exit code: defaulting it would report a container
    // that died without ever reporting status as a clean success.
    let exit_code = state.exit_code.map(|code| code as i32);

    let phase = match state.status.unwrap_or(ContainerStateStatusEnum::EMPTY) {
        ContainerStateStatusEnum::RUNNING
        | ContainerStateStatusEnum::PAUSED
        | ContainerStateStatusEnum::RESTARTING
        | ContainerStateStatusEnum::REMOVING => AttemptPhase::Running,
        ContainerStateStatusEnum::CREATED | ContainerStateStatusEnum::EMPTY => {
            AttemptPhase::Submitted
        }
        ContainerStateStatusEnum::EXITED | ContainerStateStatusEnum::DEAD
            if state.oom_killed == Some(true) =>
        {
            AttemptPhase::Failed {
                reason: "oom-killed".to_string(),
            }
        }
        ContainerStateStatusEnum::EXITED => match exit_code {
            Some(code) => AttemptPhase::Exited { code },
            None => AttemptPhase::Failed {
                reason: state
                    .error
                    .unwrap_or_else(|| "container exited without an exit code".to_string()),
            },
        },
        ContainerStateStatusEnum::DEAD => match exit_code {
            Some(code) => AttemptPhase::Exited { code },
            None => AttemptPhase::Failed {
                reason: state
                    .error
                    .unwrap_or_else(|| "container died without an exit code".to_string()),
            },
        },
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

fn classify_archive(err: &bollard::errors::Error) -> BackendError {
    use bollard::errors::Error;
    match err {
        Error::DockerResponseServerError {
            status_code,
            message,
        } => match status_code {
            400 => BackendError::InvalidSpec(message.clone()),
            404 => BackendError::NotFound(message.clone()),
            409 => BackendError::Conflict(message.clone()),
            500..=599 => BackendError::Unavailable(message.clone()),
            401 | 403 => BackendError::InvalidSpec(message.clone()),
            other => BackendError::Api(format!("archive status {other}: {message}")),
        },
        Error::IOError { .. } | Error::DockerStreamError { .. } => {
            BackendError::Unavailable(err.to_string())
        }
        _ => BackendError::Api(format!("Docker archive error: {err}")),
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
        Error::DockerStreamError { error } => classify_pull_error(error),
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
    // Range-check the whole calendar, not just the year: a malformed daemon timestamp
    // would otherwise produce a garbage epoch rather than being rejected.
    if year <= 1
        || !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || !(0..=23).contains(&hour)
        || !(0..=59).contains(&minute)
        || !(0..=60).contains(&second)
    {
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
        // Portable defaults cover CPU, memory and walltime; disk is operator configured.
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
    fn disk_default_applies() {
        let config = DockerConfig {
            default_disk_bytes: Some(2u64 << 30),
            ..DockerConfig::default()
        };
        let spec = TaskSpec::new(AttemptRef::new("j1", 0), "alpine");
        let host = build_config(&config, &spec).host_config.unwrap();
        assert_eq!(
            host.storage_opt.unwrap().get("size").unwrap(),
            &(2u64 << 30).to_string()
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
        let config = DockerConfig {
            default_disk_bytes: Some(2u64 << 30),
            ..DockerConfig::default()
        };
        let body = build_config(&config, &spec);
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
    fn pull_stream_error_classification() {
        let error = bollard::errors::Error::DockerStreamError {
            error: "manifest unknown".to_string(),
        };
        let classified = classify_pull(&error);
        assert!(matches!(classified, BackendError::ImageNotFound(_)));
        assert!(!classified.retryable());
    }

    #[test]
    fn staging_archive() {
        use crate::spec::TaskInput;

        let mut spec = TaskSpec::new(AttemptRef::new("j1", 0), "alpine");
        spec.inputs.push(TaskInput {
            path: "/data/input.txt".to_string(),
            contents: b"hello".to_vec(),
        });
        spec.output_paths.push("/results/output.txt".to_string());
        let plan = ArchivePlan::new(&spec).unwrap();
        let directories = BTreeMap::from([(PathBuf::from("results"), 0o777)]);
        let bytes = build_archive(&plan, &directories).unwrap();
        let mut archive = tar::Archive::new(Cursor::new(bytes));
        let mut found = BTreeMap::new();
        for entry in archive.entries().unwrap() {
            let mut entry = entry.unwrap();
            let path = entry.path().unwrap().into_owned();
            let mode = entry.header().mode().unwrap();
            let kind = entry.header().entry_type();
            let mut contents = Vec::new();
            entry.read_to_end(&mut contents).unwrap();
            found.insert(path, (kind, mode, contents));
        }
        assert!(!found.contains_key(Path::new("data")));
        assert_eq!(found[Path::new("results")].1 & 0o777, 0o777);
        assert_eq!(found[Path::new("data/input.txt")].1 & 0o777, 0o444);
        assert_eq!(found[Path::new("data/input.txt")].2, b"hello");
        assert!(found[Path::new("data/input.txt")].0.is_file());
    }

    #[test]
    fn path_safety() {
        use crate::spec::TaskInput;

        for path in ["relative", "/", "/../secret", "/a/./b", "/a//b"] {
            assert!(container_path(path).is_err(), "accepted unsafe path {path}");
        }
        let mut spec = TaskSpec::new(AttemptRef::new("j1", 0), "alpine");
        spec.inputs.push(TaskInput {
            path: "/data".to_string(),
            contents: Vec::new(),
        });
        spec.output_paths.push("/data/output".to_string());
        assert!(ArchivePlan::new(&spec).is_err());
    }

    fn make_archive(entries: &[(&str, tar::EntryType, &[u8])]) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        for (path, kind, contents) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(*kind);
            header.set_mode(0o644);
            header.set_size(contents.len() as u64);
            if kind.is_symlink() {
                header.set_link_name("target").unwrap();
            }
            builder.append_data(&mut header, path, *contents).unwrap();
        }
        builder.into_inner().unwrap()
    }

    #[test]
    fn output_archive() {
        let bytes = make_archive(&[("output.txt", tar::EntryType::Regular, b"done")]);
        assert_eq!(
            parse_output(
                Cursor::new(bytes.as_slice()),
                bytes.len(),
                Path::new("results/output.txt")
            )
            .unwrap(),
            b"done"
        );

        let link = make_archive(&[("output.txt", tar::EntryType::Symlink, b"")]);
        assert!(matches!(
            parse_output(
                Cursor::new(link.as_slice()),
                link.len(),
                Path::new("output.txt")
            ),
            Err(BackendError::InvalidSpec(_))
        ));
        let unexpected = make_archive(&[("other.txt", tar::EntryType::Regular, b"x")]);
        assert!(
            parse_output(
                Cursor::new(unexpected.as_slice()),
                unexpected.len(),
                Path::new("output.txt")
            )
            .is_err()
        );
        let multiple = make_archive(&[
            ("output.txt", tar::EntryType::Regular, b"x"),
            ("other.txt", tar::EntryType::Regular, b"y"),
        ]);
        assert!(
            parse_output(
                Cursor::new(multiple.as_slice()),
                multiple.len(),
                Path::new("output.txt")
            )
            .is_err()
        );
    }

    #[test]
    fn traversal_rejected() {
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Regular);
        header.set_mode(0o644);
        header.set_size(1);
        header.as_mut_bytes()[..13].copy_from_slice(b"../output.txt");
        header.set_cksum();
        let mut builder = tar::Builder::new(Vec::new());
        builder.append(&header, &b"x"[..]).unwrap();
        let bytes = builder.into_inner().unwrap();
        assert!(matches!(
            parse_output(
                Cursor::new(bytes.as_slice()),
                bytes.len(),
                Path::new("output.txt")
            ),
            Err(BackendError::InvalidSpec(_))
        ));
    }

    #[test]
    fn transfer_limits() {
        use crate::spec::TaskInput;

        let mut spec = TaskSpec::new(AttemptRef::new("j1", 0), "alpine");
        spec.inputs.push(TaskInput {
            path: "/input.txt".to_string(),
            contents: b"data".to_vec(),
        });
        let plan = ArchivePlan::new(&spec).unwrap();
        assert!(matches!(
            build_limited(&plan, &BTreeMap::new(), plan.input_bytes),
            Err(BackendError::InvalidSpec(_))
        ));

        let bytes = make_archive(&[("output.txt", tar::EntryType::Regular, b"done")]);
        assert!(matches!(
            parse_limited(
                Cursor::new(bytes.as_slice()),
                bytes.len(),
                Path::new("output.txt"),
                bytes.len() - 1,
            ),
            Err(BackendError::InvalidSpec(_))
        ));
        assert!(matches!(
            parse_limited(
                Cursor::new(bytes.as_slice()),
                bytes.len(),
                Path::new("output.txt"),
                bytes.len() + 3,
            ),
            Err(BackendError::InvalidSpec(_))
        ));
    }

    #[test]
    fn archive_classification() {
        let forbidden = bollard::errors::Error::DockerResponseServerError {
            status_code: 403,
            message: "forbidden".to_string(),
        };
        assert!(matches!(
            classify_archive(&forbidden),
            BackendError::InvalidSpec(_)
        ));
        let missing = bollard::errors::Error::DockerResponseServerError {
            status_code: 404,
            message: "missing".to_string(),
        };
        assert!(matches!(
            classify_archive(&missing),
            BackendError::NotFound(_)
        ));
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

    // A malformed calendar field must be rejected, not folded into a garbage epoch.
    #[test]
    fn rfc3339_calendar() {
        assert_eq!(parse_rfc3339_ms("2024-13-01T00:00:00Z"), None);
        assert_eq!(parse_rfc3339_ms("2024-00-01T00:00:00Z"), None);
        assert_eq!(parse_rfc3339_ms("2024-01-32T00:00:00Z"), None);
        assert_eq!(parse_rfc3339_ms("2024-01-01T24:00:00Z"), None);
        assert_eq!(parse_rfc3339_ms("2024-01-01T00:60:00Z"), None);
    }

    fn inspect_with(state: bollard::models::ContainerState) -> ContainerInspectResponse {
        ContainerInspectResponse {
            id: Some("abc".to_string()),
            state: Some(state),
            ..Default::default()
        }
    }

    // A container that died without an exit code is NOT a clean success.
    #[test]
    fn dead_without_code() {
        let status = inspect_to_status(inspect_with(bollard::models::ContainerState {
            status: Some(ContainerStateStatusEnum::DEAD),
            exit_code: None,
            ..Default::default()
        }));
        assert!(
            matches!(status.phase, AttemptPhase::Failed { .. }),
            "dead-without-exit-code must be Failed, got {:?}",
            status.phase
        );

        // A real exit code still reports as a genuine exit, zero included.
        let status = inspect_to_status(inspect_with(bollard::models::ContainerState {
            status: Some(ContainerStateStatusEnum::EXITED),
            exit_code: Some(0),
            ..Default::default()
        }));
        assert_eq!(status.phase, AttemptPhase::Exited { code: 0 });
    }

    // An OOM kill is a backend failure, not the exit code the kernel left behind.
    #[test]
    fn oom_is_failed() {
        let status = inspect_to_status(inspect_with(bollard::models::ContainerState {
            status: Some(ContainerStateStatusEnum::EXITED),
            exit_code: Some(0),
            oom_killed: Some(true),
            ..Default::default()
        }));
        assert!(
            matches!(status.phase, AttemptPhase::Failed { .. }),
            "oom-killed must be Failed, got {:?}",
            status.phase
        );
    }
}
