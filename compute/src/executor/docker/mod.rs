use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use aruna_core::compute::{
    AttemptPhase, AttemptRef, AttemptStatus, BackendError, CancelEvidence, ExecutorKind,
    InputStream, LogLimits, LogStream, LogTails, MAX_TRANSFER_BYTES, ReconcileOutcome, TaskInput,
    TaskOutput, TaskSpec,
};
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
use bollard::{Docker, body_try_stream};
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot};
use tokio_util::io::{StreamReader, SyncIoBridge};
use tokio_util::sync::CancellationToken;

use super::ExecutorBackend;
use super::config::DockerConfig;
use super::logs::{BoundedTail, LogSink};

/// Label carrying the effective walltime ceiling in milliseconds so `wait` can
/// enforce it against the daemon-reported start time.
const WALLTIME_LABEL: &str = "aruna-engine.org/max-walltime-ms";
/// Docker encodes directory type with Go's `os.ModeDir` bit.
const DIRECTORY_MODE: u32 = 1 << 31;

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

    /// Stream the staging archive into the container: a blocking task builds the
    /// tar into a bounded channel that feeds the request body chunk by chunk.
    async fn upload_archive(
        &self,
        container: &str,
        plan: &ArchivePlan<'_>,
        directories: &BTreeMap<PathBuf, u32>,
    ) -> Result<(), BackendError> {
        let mut files = Vec::with_capacity(plan.inputs.len());
        for (path, input) in &plan.inputs {
            let stream = input.take_stream().ok_or_else(|| {
                BackendError::Unavailable(format!(
                    "input `{}` stream is already consumed",
                    input.path
                ))
            })?;
            files.push((path.clone(), input.size(), stream));
        }
        let directories = directories.clone();
        let handle = Handle::current();
        let (tx, rx) = mpsc::channel(4);
        let builder = tokio::task::spawn_blocking(move || {
            build_archive(tx, handle, &directories, files, MAX_TRANSFER_BYTES)
        });
        let options = UploadToContainerOptionsBuilder::new()
            .path("/")
            .no_overwrite_dir_non_dir("true")
            .build();
        let uploaded = self
            .docker
            .upload_to_container(container, Some(options), body_try_stream(ChannelStream(rx)))
            .await;
        let built = match builder.await {
            Ok(built) => built,
            Err(join_error) => Err(BuildError::Failed(BackendError::Api(format!(
                "archive build task failed: {join_error}"
            )))),
        };
        // A build failure is authoritative even when the daemon accepted the
        // truncated body; the container is never started on this path.
        match (built, uploaded) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(BuildError::Failed(error)), _) => Err(error),
            (_, Err(error)) => Err(classify_archive(&error)),
            (Err(BuildError::Aborted), Ok(())) => Err(BackendError::Api(
                "archive upload ended before the archive was complete".to_string(),
            )),
        }
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
                let uploaded = self.upload_archive(container_id, plan, &directories).await;
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
}

impl<'a> ArchivePlan<'a> {
    fn new(spec: &'a TaskSpec) -> Result<Self, BackendError> {
        let mut inputs = BTreeMap::new();
        let mut outputs = BTreeSet::new();
        let mut files = BTreeSet::new();
        let mut input_bytes = 0u64;
        for input in &spec.inputs {
            let path = container_path(&input.path)?;
            if inputs.insert(path.clone(), input).is_some() || !files.insert(path) {
                return Err(BackendError::InvalidSpec(format!(
                    "duplicate input path `{}`",
                    input.path
                )));
            }
            input_bytes = input_bytes
                .checked_add(input.size())
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
        })
    }
}

/// Marker smuggled through `io::Error` when a streaming transfer passes the
/// aggregate byte guard.
#[derive(Debug)]
struct TransferLimitError;

impl std::fmt::Display for TransferLimitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "task file transfer exceeds the {MAX_TRANSFER_BYTES}-byte limit"
        )
    }
}

impl std::error::Error for TransferLimitError {}

fn transfer_error() -> BackendError {
    BackendError::InvalidSpec(TransferLimitError.to_string())
}

/// `mpsc::Receiver` as a `Stream`; carries archive chunks across task borders.
struct ChannelStream<T>(mpsc::Receiver<T>);

impl<T> Stream for ChannelStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.0.poll_recv(cx)
    }
}

/// Byte-counting tar sink feeding the upload body channel.
struct ChannelWriter {
    tx: mpsc::Sender<io::Result<Bytes>>,
    sent: u64,
    limit: u64,
}

impl Write for ChannelWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.sent = self
            .sent
            .checked_add(bytes.len() as u64)
            .filter(|total| *total <= self.limit)
            .ok_or_else(|| io::Error::other(TransferLimitError))?;
        self.tx
            .blocking_send(Ok(Bytes::copy_from_slice(bytes)))
            .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Guards a tar entry body: the stream must yield exactly the declared size or
/// the archive would be silently corrupt.
struct ExactReader<R> {
    inner: R,
    remaining: u64,
}

impl<R: Read> Read for ExactReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.remaining == 0 {
            let mut probe = [0u8; 1];
            if self.inner.read(&mut probe)? != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "input is larger than its declared size",
                ));
            }
            return Ok(0);
        }
        let cap = buffer
            .len()
            .min(usize::try_from(self.remaining).unwrap_or(usize::MAX));
        let read = self.inner.read(&mut buffer[..cap])?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "input ended before its declared size",
            ));
        }
        self.remaining -= read as u64;
        Ok(read)
    }
}

enum BuildError {
    /// The body receiver went away; the upload error is authoritative.
    Aborted,
    Failed(BackendError),
}

fn input_error(error: io::Error) -> BuildError {
    if error.kind() == io::ErrorKind::BrokenPipe {
        return BuildError::Aborted;
    }
    if error
        .get_ref()
        .is_some_and(|inner| inner.is::<TransferLimitError>())
    {
        return BuildError::Failed(transfer_error());
    }
    BuildError::Failed(BackendError::Unavailable(format!(
        "task input transfer failed: {error}"
    )))
}

fn build_archive(
    tx: mpsc::Sender<io::Result<Bytes>>,
    handle: Handle,
    directories: &BTreeMap<PathBuf, u32>,
    files: Vec<(PathBuf, u64, InputStream)>,
    limit: u64,
) -> Result<(), BuildError> {
    let writer = ChannelWriter {
        tx: tx.clone(),
        sent: 0,
        limit,
    };
    let built = write_entries(writer, handle, directories, files);
    // An explicit error chunk aborts the request; a clean close of a truncated
    // body could otherwise be accepted by the daemon as a complete archive.
    if let Err(BuildError::Failed(_)) = &built {
        let _ = tx.blocking_send(Err(io::Error::other("task archive build failed")));
    }
    built
}

fn write_entries(
    writer: ChannelWriter,
    handle: Handle,
    directories: &BTreeMap<PathBuf, u32>,
    files: Vec<(PathBuf, u64, InputStream)>,
) -> Result<(), BuildError> {
    let mut builder = tar::Builder::new(writer);
    for (path, mode) in directories {
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Directory);
        header.set_mode(*mode);
        header.set_uid(0);
        header.set_gid(0);
        header.set_mtime(0);
        header.set_size(0);
        builder
            .append_data(&mut header, path, io::empty())
            .map_err(input_error)?;
    }
    for (path, size, stream) in files {
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Regular);
        header.set_mode(0o444);
        header.set_uid(0);
        header.set_gid(0);
        header.set_mtime(0);
        header.set_size(size);
        let reader = ExactReader {
            inner: SyncIoBridge::new_with_handle(StreamReader::new(stream), handle.clone()),
            remaining: size,
        };
        builder
            .append_data(&mut header, path, reader)
            .map_err(input_error)?;
    }
    builder.finish().map_err(input_error)
}

fn archive_api(error: impl std::fmt::Display) -> BackendError {
    BackendError::InvalidSpec(format!("invalid Docker output archive: {error}"))
}

fn output_spec(message: impl Into<String>) -> BackendError {
    BackendError::InvalidSpec(format!("invalid Docker output: {}", message.into()))
}

/// Recover the classification smuggled through the archive reader: transport
/// faults keep their classified form, everything else is a malformed archive.
fn archive_error(error: io::Error) -> BackendError {
    if error
        .get_ref()
        .is_some_and(|inner| inner.is::<TransferLimitError>())
    {
        return transfer_error();
    }
    match error
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<BackendError>())
    {
        Some(backend) => backend.clone(),
        None => archive_api(error),
    }
}

/// Enforce the aggregate byte guard on raw archive bytes as they stream.
fn count_limited<S>(stream: S, limit: u64) -> impl Stream<Item = io::Result<Bytes>>
where
    S: Stream<Item = io::Result<Bytes>>,
{
    let mut total = 0u64;
    stream.map(move |chunk| {
        let chunk = chunk?;
        total = total
            .checked_add(chunk.len() as u64)
            .filter(|total| *total <= limit)
            .ok_or_else(|| io::Error::other(TransferLimitError))?;
        Ok(chunk)
    })
}

fn first_entry<'a, R: Read>(
    entries: &mut tar::Entries<'a, R>,
    expected: &Path,
) -> Result<tar::Entry<'a, R>, BackendError> {
    let entry = entries
        .next()
        .ok_or_else(|| output_spec("archive is empty"))?
        .map_err(archive_error)?;
    {
        let path = entry.path().map_err(archive_api)?;
        if path.is_absolute()
            || path
                .components()
                .any(|part| !matches!(part, std::path::Component::Normal(_)))
        {
            return Err(output_spec("archive entry path is unsafe"));
        }
        if path != expected {
            return Err(output_spec(format!(
                "archive contains unexpected entry `{}`",
                path.display()
            )));
        }
    }
    if !entry.header().entry_type().is_file() || entry.link_name().map_err(archive_api)?.is_some() {
        return Err(output_spec("declared output is not a regular file"));
    }
    Ok(entry)
}

/// Blocking tar parse: validate the single expected entry, hand its size to the
/// header channel, then stream its bytes chunkwise.
fn stream_output(
    archive: impl Read,
    expected: &Path,
    header: oneshot::Sender<Result<u64, BackendError>>,
    tx: mpsc::Sender<Result<Bytes, BackendError>>,
) {
    let mut archive = tar::Archive::new(archive);
    let mut entries = match archive.entries().map_err(archive_error) {
        Ok(entries) => entries,
        Err(error) => {
            let _ = header.send(Err(error));
            return;
        }
    };
    let mut entry = match first_entry(&mut entries, expected) {
        Ok(entry) => entry,
        Err(error) => {
            let _ = header.send(Err(error));
            return;
        }
    };
    let size = entry.size();
    if header.send(Ok(size)).is_err() {
        return;
    }
    let mut streamed = 0u64;
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        match entry.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => {
                streamed += read as u64;
                if tx
                    .blocking_send(Ok(Bytes::copy_from_slice(&buffer[..read])))
                    .is_err()
                {
                    return;
                }
            }
            Err(error) => {
                let _ = tx.blocking_send(Err(archive_error(error)));
                return;
            }
        }
    }
    if streamed != size {
        let _ = tx.blocking_send(Err(output_spec("archive entry is truncated")));
        return;
    }
    drop(entry);
    match entries.next() {
        None => {}
        Some(Ok(_)) => {
            let _ = tx.blocking_send(Err(output_spec("archive contains multiple entries")));
        }
        Some(Err(error)) => {
            let _ = tx.blocking_send(Err(archive_error(error)));
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
    ) -> Result<TaskOutput, BackendError> {
        attempt.validate().map_err(BackendError::InvalidSpec)?;
        let output_path = container_path(path)?;
        let expected = output_path
            .file_name()
            .map(PathBuf::from)
            .ok_or_else(|| output_spec("path has no file name"))?;
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
        let raw = self
            .docker
            .download_from_container(container_id, Some(options))
            .map(|chunk| chunk.map_err(|error| io::Error::other(classify_archive(&error))));
        let counted = Box::pin(count_limited(raw, MAX_TRANSFER_BYTES));
        let reader = SyncIoBridge::new_with_handle(StreamReader::new(counted), Handle::current());
        let (header_tx, header_rx) = oneshot::channel();
        let (tx, rx) = mpsc::channel(4);
        tokio::task::spawn_blocking(move || stream_output(reader, &expected, header_tx, tx));
        match header_rx.await {
            Ok(Ok(size)) => Ok(TaskOutput {
                size,
                chunks: Box::pin(ChannelStream(rx)),
            }),
            Ok(Err(error)) => {
                if matches!(&error, BackendError::NotFound(_)) {
                    return match self.inspect_matching_attempt(attempt).await {
                        Ok(_) => Err(output_spec(format!("path `{path}` does not exist"))),
                        Err(error) => Err(error),
                    };
                }
                Err(error)
            }
            Err(_) => Err(BackendError::Api(
                "output parse stopped before the tar header".to_string(),
            )),
        }
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

    /// Drive the blocking archive builder over `plan` and collect the tar bytes.
    async fn collect_archive(
        plan: &ArchivePlan<'_>,
        directories: &BTreeMap<PathBuf, u32>,
        limit: u64,
    ) -> Result<Vec<u8>, BackendError> {
        let mut files = Vec::new();
        for (path, input) in &plan.inputs {
            files.push((path.clone(), input.size(), input.take_stream().unwrap()));
        }
        let directories = directories.clone();
        let handle = Handle::current();
        let (tx, mut rx) = mpsc::channel(4);
        let task = tokio::task::spawn_blocking(move || {
            build_archive(tx, handle, &directories, files, limit)
        });
        let mut bytes = Vec::new();
        while let Some(chunk) = rx.recv().await {
            if let Ok(chunk) = chunk {
                bytes.extend_from_slice(&chunk);
            }
        }
        match task.await.unwrap() {
            Ok(()) => Ok(bytes),
            Err(BuildError::Failed(error)) => Err(error),
            Err(BuildError::Aborted) => panic!("archive build aborted"),
        }
    }

    /// Run the streaming output parser over in-memory archive bytes.
    async fn parse_bytes(
        archive: Vec<u8>,
        expected: &Path,
        limit: u64,
    ) -> Result<(u64, Vec<u8>), BackendError> {
        let raw = futures_util::stream::iter([Ok::<_, io::Error>(Bytes::from(archive))]);
        let counted = Box::pin(count_limited(raw, limit));
        let reader = SyncIoBridge::new_with_handle(StreamReader::new(counted), Handle::current());
        let (header_tx, header_rx) = oneshot::channel();
        let (tx, mut rx) = mpsc::channel(4);
        let expected = expected.to_path_buf();
        tokio::task::spawn_blocking(move || stream_output(reader, &expected, header_tx, tx));
        let size = header_rx.await.expect("header result")?;
        let mut bytes = Vec::new();
        while let Some(chunk) = rx.recv().await {
            bytes.extend_from_slice(&chunk?);
        }
        Ok((size, bytes))
    }

    fn read_entries(bytes: Vec<u8>) -> BTreeMap<PathBuf, (tar::EntryType, u32, Vec<u8>)> {
        let mut archive = tar::Archive::new(io::Cursor::new(bytes));
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
        found
    }

    #[tokio::test]
    async fn staging_archive() {
        let mut spec = TaskSpec::new(AttemptRef::new("j1", 0), "alpine");
        spec.inputs
            .push(TaskInput::from_bytes("/data/input.txt", b"hello".to_vec()));
        spec.output_paths.push("/results/output.txt".to_string());
        let plan = ArchivePlan::new(&spec).unwrap();
        let directories = BTreeMap::from([(PathBuf::from("results"), 0o777)]);
        let bytes = collect_archive(&plan, &directories, MAX_TRANSFER_BYTES)
            .await
            .unwrap();
        let found = read_entries(bytes);
        assert!(!found.contains_key(Path::new("data")));
        assert_eq!(found[Path::new("results")].1 & 0o777, 0o777);
        assert_eq!(found[Path::new("data/input.txt")].1 & 0o777, 0o444);
        assert_eq!(found[Path::new("data/input.txt")].2, b"hello");
        assert!(found[Path::new("data/input.txt")].0.is_file());
    }

    #[tokio::test]
    async fn chunked_input() {
        // A multi-chunk input stream must land as one contiguous archive entry.
        let mut contents = vec![b'a'; 700];
        contents.extend_from_slice(&[b'b'; 300]);
        contents.extend_from_slice(&[b'c'; 24]);
        let chunks: Vec<io::Result<Bytes>> = vec![
            Ok(Bytes::from(vec![b'a'; 700])),
            Ok(Bytes::from(vec![b'b'; 300])),
            Ok(Bytes::from(vec![b'c'; 24])),
        ];
        let mut spec = TaskSpec::new(AttemptRef::new("j1", 0), "alpine");
        spec.inputs.push(TaskInput::from_stream(
            "/data/blob.bin",
            contents.len() as u64,
            Box::pin(futures_util::stream::iter(chunks)),
        ));
        let plan = ArchivePlan::new(&spec).unwrap();
        let bytes = collect_archive(&plan, &BTreeMap::new(), MAX_TRANSFER_BYTES)
            .await
            .unwrap();
        let found = read_entries(bytes);
        assert_eq!(found[Path::new("data/blob.bin")].2, contents);
    }

    #[tokio::test]
    async fn size_mismatch_fails() {
        // A stream shorter or longer than its declared size must not corrupt the tar.
        for (declared, actual) in [(10u64, 5usize), (5, 10)] {
            let chunk: Vec<io::Result<Bytes>> = vec![Ok(Bytes::from(vec![b'x'; actual]))];
            let mut spec = TaskSpec::new(AttemptRef::new("j1", 0), "alpine");
            spec.inputs.push(TaskInput::from_stream(
                "/data/blob.bin",
                declared,
                Box::pin(futures_util::stream::iter(chunk)),
            ));
            let plan = ArchivePlan::new(&spec).unwrap();
            let result = collect_archive(&plan, &BTreeMap::new(), MAX_TRANSFER_BYTES).await;
            assert!(
                matches!(result, Err(BackendError::Unavailable(_))),
                "declared {declared} actual {actual} must fail"
            );
        }
    }

    #[test]
    fn oversized_plan() {
        // Declared sizes above the 4 GiB aggregate guard reject before any I/O.
        let mut spec = TaskSpec::new(AttemptRef::new("j1", 0), "alpine");
        spec.inputs.push(TaskInput::from_stream(
            "/big.bin",
            MAX_TRANSFER_BYTES + 1,
            Box::pin(futures_util::stream::empty()),
        ));
        assert!(matches!(
            ArchivePlan::new(&spec),
            Err(BackendError::InvalidSpec(_))
        ));
    }

    #[test]
    fn path_safety() {
        for path in ["relative", "/", "/../secret", "/a/./b", "/a//b"] {
            assert!(container_path(path).is_err(), "accepted unsafe path {path}");
        }
        let mut spec = TaskSpec::new(AttemptRef::new("j1", 0), "alpine");
        spec.inputs
            .push(TaskInput::from_bytes("/data", Bytes::new()));
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

    #[tokio::test]
    async fn output_archive() {
        let bytes = make_archive(&[("output.txt", tar::EntryType::Regular, b"done")]);
        let (size, contents) = parse_bytes(bytes, Path::new("output.txt"), MAX_TRANSFER_BYTES)
            .await
            .unwrap();
        assert_eq!(size, 4);
        assert_eq!(contents, b"done");

        let link = make_archive(&[("output.txt", tar::EntryType::Symlink, b"")]);
        assert!(matches!(
            parse_bytes(link, Path::new("output.txt"), MAX_TRANSFER_BYTES).await,
            Err(BackendError::InvalidSpec(_))
        ));
        let unexpected = make_archive(&[("other.txt", tar::EntryType::Regular, b"x")]);
        assert!(
            parse_bytes(unexpected, Path::new("output.txt"), MAX_TRANSFER_BYTES)
                .await
                .is_err()
        );
        let multiple = make_archive(&[
            ("output.txt", tar::EntryType::Regular, b"x"),
            ("other.txt", tar::EntryType::Regular, b"y"),
        ]);
        assert!(
            parse_bytes(multiple, Path::new("output.txt"), MAX_TRANSFER_BYTES)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn traversal_rejected() {
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
            parse_bytes(bytes, Path::new("output.txt"), MAX_TRANSFER_BYTES).await,
            Err(BackendError::InvalidSpec(_))
        ));
    }

    #[tokio::test]
    async fn transfer_limits() {
        // The aggregate guard trips while bytes stream, on build and on parse.
        let mut spec = TaskSpec::new(AttemptRef::new("j1", 0), "alpine");
        spec.inputs
            .push(TaskInput::from_bytes("/input.txt", b"data".to_vec()));
        let plan = ArchivePlan::new(&spec).unwrap();
        assert!(matches!(
            collect_archive(&plan, &BTreeMap::new(), 100).await,
            Err(BackendError::InvalidSpec(_))
        ));

        let bytes = make_archive(&[("output.txt", tar::EntryType::Regular, b"done")]);
        let limit = (bytes.len() - 1) as u64;
        assert!(matches!(
            parse_bytes(bytes, Path::new("output.txt"), limit).await,
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
