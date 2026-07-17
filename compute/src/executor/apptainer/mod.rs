use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::{Command, Stdio};
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use aruna_core::compute::{
    AdoptableEvidence, ArtifactEvidence, AttemptPhase, AttemptStatus, BackendError, CancelEvidence,
    ExecutorKind, FenceContext, LogLimits, LogStream, LogTails, MAX_TRANSFER_BYTES, NetworkAccess,
    ReconcileEvidence, ResumePoint, StagingMode, TaskOutput, TaskSpec, TombstoneEvidence,
    TombstoneSpec, UserSpec, normalize_container_path,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command as TokioCommand;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::config::ApptainerConfig;
use super::logs::{BoundedTail, LogSink};
use super::staging::{StageLayout, StagePlan};
use super::{BackendCaps, ExecutorBackend, digest_pinned};

mod runtime;
mod state;

use state::{
    AttemptRecord, BindMount, ControlRecord, LaunchRecord, OciMetadata, PayloadRecord,
    ProcessRecord, StateRoot, StatusRecord, read_json, read_optional, sync_dir, write_json,
};

pub struct ApptainerBackend {
    config: ApptainerConfig,
    state: StateRoot,
}

impl ApptainerBackend {
    pub fn with_config(config: ApptainerConfig) -> Result<Self, BackendError> {
        let state = StateRoot::open(&config.state_root)?;
        std::fs::create_dir_all(&config.sif_cache).map_err(io_error)?;
        Ok(Self { config, state })
    }

    pub fn config(&self) -> &ApptainerConfig {
        &self.config
    }

    fn attempt_status(&self, context: &FenceContext) -> Result<AttemptStatus, BackendError> {
        validate_control(self.state.read(context)?, context)?;
        let directory = self.state.attempt_dir(context);
        if let Some(status) = read_optional::<StatusRecord>(&directory.join("status.json"))? {
            return Ok(to_status(&directory, status));
        }
        if let Some(payload) = read_optional::<PayloadRecord>(&directory.join("payload.json"))? {
            if runtime::process_live(&payload.process)? {
                return Ok(running_status(&directory));
            }
            if !runtime::cgroup_empty(&payload.cgroup)? {
                return Ok(running_status(&directory));
            }
            return settle_status(&directory, running_status(&directory));
        }
        if directory.join("supervisor.json").exists() {
            return settle_status(&directory, submitted_status(&directory));
        }
        Err(BackendError::NotFound(context.attempt.external_name()))
    }

    fn existing_status(
        &self,
        context: &FenceContext,
    ) -> Result<Option<AttemptStatus>, BackendError> {
        match self.attempt_status(context) {
            Ok(status) => Ok(Some(status)),
            Err(BackendError::NotFound(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn cgroup_path(&self, context: &FenceContext) -> PathBuf {
        self.config
            .cgroup_root
            .join(context.attempt.external_name())
    }

    async fn prepare_attempt(
        &self,
        context: &FenceContext,
        spec: &TaskSpec,
        sif: PathBuf,
        metadata: OciMetadata,
    ) -> Result<PathBuf, BackendError> {
        let plan = StagePlan::from_spec(spec)?;
        let directory = self.state.attempt_dir(context);
        let temp = directory.with_extension(format!("{}.tmp", context.controller_generation));
        remove_staging_temps(&directory)?;
        std::fs::create_dir_all(temp.join("workspace/root")).map_err(io_error)?;
        std::fs::create_dir_all(temp.join("logs")).map_err(io_error)?;
        let layout_digest = plan.layout.digest();
        let binds = stage_files(&temp, plan).await?;
        let argv = command_argv(spec, &metadata)?;
        let record = AttemptRecord {
            attempt_epoch: context.attempt_epoch,
            pinned_image: spec.image.clone(),
            layout_digest,
            output_paths: spec.output_paths.clone(),
        };
        let launch = LaunchRecord {
            sif,
            argv,
            binds,
            workdir: spec.workdir.clone(),
            cgroup: self.cgroup_path(context),
            control: self.state.control_dir(context).join("control.json"),
            stop_grace_ms: self
                .config
                .stop_grace
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX),
            walltime_ms: spec
                .resources
                .max_walltime
                .map(|duration| duration.as_millis().try_into().unwrap_or(u64::MAX)),
            pids_limit: spec.security.pids_limit.unwrap_or(2048),
            memory_bytes: spec.resources.ram_bytes,
            cpu_cores: spec.resources.cpu_cores,
            isolated_network: spec.security.network == NetworkAccess::Isolated,
        };
        write_json(&temp.join("attempt.json"), &record)?;
        write_json(&temp.join("launch.json"), &launch)?;
        sync_dir(&temp)?;
        remove_tree(&directory)?;
        std::fs::rename(&temp, &directory).map_err(io_error)?;
        sync_dir(
            directory
                .parent()
                .ok_or_else(|| BackendError::Api("attempt path has no parent".to_string()))?,
        )?;
        Ok(directory)
    }

    async fn spawn_supervisor(
        &self,
        directory: &Path,
        spec: &TaskSpec,
        cancel: &CancellationToken,
    ) -> Result<AttemptStatus, BackendError> {
        let mut command = Command::new(std::env::current_exe().map_err(io_error)?);
        command
            .arg("apptainer-supervisor")
            .arg(directory)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        for (key, _) in std::env::vars_os() {
            if key.to_string_lossy().starts_with("APPTAINERENV_") {
                command.env_remove(key);
            }
        }
        for (key, value) in spec.effective_env() {
            validate_env_key(&key)?;
            command.env(format!("APPTAINERENV_{key}"), value);
        }
        let mut child = command.spawn().map_err(io_error)?;
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if cancel.is_cancelled() {
                let _ = child.kill();
                return Err(BackendError::Cancelled);
            }
            if directory.join("supervisor.json").exists() {
                return Ok(submitted_status(directory));
            }
            if let Some(status) = child.try_wait().map_err(io_error)? {
                return Err(BackendError::Api(format!(
                    "Apptainer supervisor exited before readiness: {status}"
                )));
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                return Err(BackendError::Timeout(
                    "Apptainer supervisor readiness timed out".to_string(),
                ));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn ensure_sif(
        &self,
        image: &str,
        cancel: &CancellationToken,
    ) -> Result<(PathBuf, OciMetadata), BackendError> {
        let digest = image
            .rsplit_once("@sha256:")
            .map(|(_, digest)| digest)
            .ok_or_else(|| BackendError::InvalidSpec("image must be digest-pinned".to_string()))?;
        let sif = self.config.sif_cache.join(format!("{digest}.sif"));
        let metadata_path = self.config.sif_cache.join(format!("{digest}.json"));
        let lock_path = self.config.sif_cache.join(format!("{digest}.lock"));
        let _lock = tokio::task::spawn_blocking(move || -> Result<File, BackendError> {
            let lock = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(lock_path)
                .map_err(io_error)?;
            lock.lock().map_err(io_error)?;
            Ok(lock)
        })
        .await
        .map_err(|error| BackendError::Api(format!("Apptainer lock task failed: {error}")))??;
        if sif.exists() && metadata_path.exists() {
            return Ok((sif, read_json(&metadata_path)?));
        }
        if cancel.is_cancelled() {
            return Err(BackendError::Cancelled);
        }
        let temp = self.config.sif_cache.join(format!("{digest}.sif.tmp"));
        let mut command = TokioCommand::new("apptainer");
        command
            .args(["pull", "--disable-cache", "--force"])
            .arg(&temp)
            .arg(format!("docker://{image}"))
            .kill_on_drop(true);
        let status = tokio::select! {
            _ = cancel.cancelled() => return Err(BackendError::Cancelled),
            result = tokio::time::timeout(self.config.pull_deadline, command.status()) => {
                result
                    .map_err(|_| BackendError::Timeout("Apptainer image pull timed out".to_string()))?
                    .map_err(io_error)?
            }
        };
        if !status.success() {
            return Err(BackendError::Api(format!(
                "Apptainer image pull failed with {status}"
            )));
        }
        File::open(&temp)
            .and_then(|file| file.sync_all())
            .map_err(io_error)?;
        std::fs::rename(&temp, &sif).map_err(io_error)?;
        sync_dir(&self.config.sif_cache)?;
        let metadata = inspect_metadata(&sif).await?;
        write_json(&metadata_path, &metadata)?;
        Ok((sif, metadata))
    }
}

#[async_trait]
impl ExecutorBackend for ApptainerBackend {
    fn kind(&self) -> ExecutorKind {
        ExecutorKind::Apptainer
    }

    fn capabilities(&self) -> BackendCaps {
        BackendCaps {
            file_staging: true,
            direct_s3: true,
        }
    }

    /// Apptainer performs no user switch, so tasks inherit the service identity.
    fn run_identity(&self) -> UserSpec {
        process_identity()
    }

    async fn health(&self) -> Result<(), BackendError> {
        let output = TokioCommand::new("apptainer")
            .arg("version")
            .output()
            .await
            .map_err(io_error)?;
        if !output.status.success() || output.stdout.is_empty() {
            return Err(BackendError::Unavailable(
                "Apptainer version check failed".to_string(),
            ));
        }
        if is_root(process_identity()) {
            return Err(BackendError::Unavailable(
                "Apptainer executor must run as a non-root user".to_string(),
            ));
        }
        self.state.verify()?;
        runtime::probe_cgroup(&self.config.cgroup_root)
    }

    async fn resolve_image(
        &self,
        image: &str,
        cancel: &CancellationToken,
    ) -> Result<String, BackendError> {
        if digest_pinned(image) {
            Ok(image.to_string())
        } else {
            resolve_digest(image, self.config.pull_deadline, cancel).await
        }
    }

    async fn fence(&self, context: &FenceContext) -> Result<(), BackendError> {
        context
            .attempt
            .validate()
            .map_err(BackendError::InvalidSpec)?;
        self.state.control(context).map(|_| ())
    }

    async fn submit(
        &self,
        context: &FenceContext,
        spec: &TaskSpec,
        cancel: &CancellationToken,
    ) -> Result<AttemptStatus, BackendError> {
        validate_spec(context, spec)?;
        let guard = self.state.control(context)?;
        if guard.tombstone().is_some() {
            return Err(BackendError::Conflict("attempt is tombstoned".to_string()));
        }
        if let Some(status) = self.existing_status(context)? {
            if matches!(status.phase, AttemptPhase::Failed { ref reason } if reason.contains("lost evidence"))
            {
                return Ok(status);
            }
            let payload = self.state.attempt_dir(context).join("payload.json");
            let supervisor = self.state.attempt_dir(context).join("supervisor.json");
            if payload.exists()
                || read_optional::<ProcessRecord>(&supervisor)?
                    .is_some_and(|record| runtime::process_live(&record).unwrap_or(false))
                || status.is_terminal()
            {
                return Ok(status);
            }
        }
        if !runtime::cgroup_empty(&self.cgroup_path(context))? {
            return Err(BackendError::Conflict(
                "attempt cgroup contains an unowned process".to_string(),
            ));
        }
        if cancel.is_cancelled() {
            return Err(BackendError::Cancelled);
        }
        let (sif, metadata) = self.ensure_sif(&spec.image, cancel).await?;
        let directory = self.prepare_attempt(context, spec, sif, metadata).await?;
        self.spawn_supervisor(&directory, spec, cancel).await
    }

    async fn stage(
        &self,
        _context: &FenceContext,
        _spec: &TaskSpec,
        _cancel: &CancellationToken,
    ) -> Result<(), BackendError> {
        Err(BackendError::InvalidSpec(
            "Apptainer stages during submit".to_string(),
        ))
    }

    async fn unsuspend(
        &self,
        _context: &FenceContext,
        _cancel: &CancellationToken,
    ) -> Result<AttemptStatus, BackendError> {
        Err(BackendError::InvalidSpec(
            "Apptainer starts during submit".to_string(),
        ))
    }

    async fn status(&self, context: &FenceContext) -> Result<AttemptStatus, BackendError> {
        self.attempt_status(context)
    }

    async fn cancel(&self, context: &FenceContext) -> Result<CancelEvidence, BackendError> {
        let mut guard = self.state.control(context)?;
        let status = match self.existing_status(context)? {
            Some(status) if matches!(status.phase, AttemptPhase::Failed { ref reason } if reason.contains("lost evidence")) =>
            {
                guard.mark_cancel()?;
                return Ok(CancelEvidence::Stopped(AttemptStatus {
                    phase: AttemptPhase::Cancelled,
                    ..status
                }));
            }
            Some(status) if status.is_terminal() => return Ok(CancelEvidence::Stopped(status)),
            Some(status) => status,
            None => return Ok(CancelEvidence::AlreadyGone),
        };
        guard.mark_cancel()?;
        let directory = self.state.attempt_dir(context);
        let payload = read_optional::<PayloadRecord>(&directory.join("payload.json"))?;
        let cgroup = self.cgroup_path(context);
        let grace = self.config.stop_grace;
        tokio::task::spawn_blocking(move || runtime::terminate(&cgroup, payload.as_ref(), grace))
            .await
            .map_err(|error| {
                BackendError::Api(format!("Apptainer terminate task failed: {error}"))
            })??;
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if let Some(status) = self.existing_status(context)?
                && status.is_terminal()
            {
                return Ok(CancelEvidence::Stopped(status));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        if matches!(
            status.phase,
            AttemptPhase::Running | AttemptPhase::Submitted
        ) {
            Ok(CancelEvidence::Requested)
        } else {
            Ok(CancelEvidence::Stopped(status))
        }
    }

    async fn fetch_logs(
        &self,
        context: &FenceContext,
        limits: &LogLimits,
        sink: &dyn LogSink,
    ) -> Result<LogTails, BackendError> {
        validate_control(self.state.read(context)?, context)?;
        let logs = self.state.attempt_dir(context).join("logs");
        let stdout = read_log(&logs.join("stdout"), LogStream::Stdout, limits, sink).await?;
        let stderr = read_log(&logs.join("stderr"), LogStream::Stderr, limits, sink).await?;
        Ok(LogTails {
            stdout: stdout.0,
            stderr: stderr.0,
            stdout_total: stdout.1,
            stderr_total: stderr.1,
            stdout_truncated: stdout.2,
            stderr_truncated: stderr.2,
        })
    }

    async fn fetch_output(
        &self,
        context: &FenceContext,
        path: &str,
    ) -> Result<TaskOutput, BackendError> {
        let status = self.attempt_status(context)?;
        if !status.is_terminal() {
            return Err(BackendError::InvalidSpec(
                "attempt is not terminal".to_string(),
            ));
        }
        let directory = self.state.attempt_dir(context);
        let attempt: AttemptRecord = read_json(&directory.join("attempt.json"))?;
        if !attempt.output_paths.iter().any(|output| output == path) {
            return Err(BackendError::InvalidSpec(
                "output path was not declared".to_string(),
            ));
        }
        let normalized = normalize_container_path(path).map_err(BackendError::InvalidSpec)?;
        let root = directory
            .join("workspace/root")
            .canonicalize()
            .map_err(io_error)?;
        let candidate = host_path(&root, &normalized)
            .canonicalize()
            .map_err(io_error)?;
        if !candidate.starts_with(&root) || !candidate.is_file() {
            return Err(BackendError::InvalidSpec(
                "output is not a regular workspace file".to_string(),
            ));
        }
        let size = candidate.metadata().map_err(io_error)?.len();
        if size > MAX_TRANSFER_BYTES {
            return Err(BackendError::InvalidSpec(
                "output exceeds transfer limit".to_string(),
            ));
        }
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(stream_file(candidate, tx));
        Ok(TaskOutput {
            size,
            chunks: Box::pin(ChannelStream(rx)),
        })
    }

    async fn reconcile(&self, context: &FenceContext) -> ReconcileEvidence {
        let control = match self.state.read(context) {
            Ok(control) => control,
            Err(error) => return ReconcileEvidence::Unavailable(error),
        };
        if let Err(error) = validate_control(control.clone(), context) {
            return ReconcileEvidence::Unavailable(error);
        }
        if let Some(reference) = control.and_then(|record| record.tombstone_ref) {
            return ReconcileEvidence::Tombstoned(TombstoneEvidence {
                backend_ref: reference,
                attempt_epoch: context.attempt_epoch,
            });
        }
        let payload = match read_optional::<PayloadRecord>(
            &self.state.attempt_dir(context).join("payload.json"),
        ) {
            Ok(payload) => payload,
            Err(error) => return ReconcileEvidence::Unavailable(error),
        };
        if let Some(payload) = payload {
            let live = match runtime::process_live(&payload.process) {
                Ok(live) => live,
                Err(error) => return ReconcileEvidence::Unavailable(error),
            };
            if !live {
                match runtime::cgroup_empty(&payload.cgroup) {
                    Ok(false) => {
                        return ReconcileEvidence::Unadoptable(ArtifactEvidence {
                            artifact_kind: "apptainer-cgroup".to_string(),
                            backend_ref: Some(payload.cgroup.display().to_string()),
                            observed_epoch: Some(context.attempt_epoch),
                            observed_generation: Some(context.controller_generation),
                            exact_identity: true,
                            multiple: false,
                            foreign: false,
                        });
                    }
                    Ok(true) => {}
                    Err(error) => return ReconcileEvidence::Unavailable(error),
                }
            }
        }
        match self.attempt_status(context) {
            Ok(status) => {
                let resume = if matches!(status.phase, AttemptPhase::Submitted) {
                    ResumePoint::Submit
                } else {
                    ResumePoint::Observe
                };
                ReconcileEvidence::Adoptable(AdoptableEvidence { status, resume })
            }
            Err(BackendError::NotFound(_)) => {
                let cgroup = self.cgroup_path(context);
                match runtime::cgroup_empty(&cgroup) {
                    Ok(true) => ReconcileEvidence::Absent,
                    Ok(false) => ReconcileEvidence::Unadoptable(ArtifactEvidence {
                        artifact_kind: "apptainer-cgroup".to_string(),
                        backend_ref: Some(cgroup.display().to_string()),
                        observed_epoch: None,
                        observed_generation: None,
                        exact_identity: false,
                        multiple: false,
                        foreign: true,
                    }),
                    Err(error) => ReconcileEvidence::Unavailable(error),
                }
            }
            Err(error) => ReconcileEvidence::Unavailable(error),
        }
    }

    async fn tombstone(
        &self,
        context: &FenceContext,
        _spec: &TombstoneSpec,
    ) -> Result<TombstoneEvidence, BackendError> {
        let mut guard = self.state.control(context)?;
        if let Some(tombstone) = guard.tombstone() {
            return Ok(tombstone);
        }
        if !runtime::cgroup_empty(&self.cgroup_path(context))? {
            return Err(BackendError::Conflict(
                "cannot tombstone a live Apptainer cgroup".to_string(),
            ));
        }
        let directory = self.state.attempt_dir(context);
        if let Some(payload) = read_optional::<PayloadRecord>(&directory.join("payload.json"))?
            && runtime::process_live(&payload.process)?
        {
            return Err(BackendError::Conflict(
                "cannot tombstone a live Apptainer payload".to_string(),
            ));
        }
        let reference = self
            .state
            .control_dir(context)
            .join("tombstone.json")
            .display()
            .to_string();
        write_json(
            &PathBuf::from(&reference),
            &BTreeMap::from([
                ("attempt_epoch", context.attempt_epoch.to_string()),
                ("external_name", context.attempt.external_name()),
            ]),
        )?;
        remove_tree(&directory)?;
        remove_cgroup(&self.cgroup_path(context))?;
        guard.seal(reference)
    }

    async fn cleanup(&self, context: &FenceContext) -> Result<(), BackendError> {
        let _guard = self.state.control(context)?;
        let status = self.existing_status(context)?;
        if (status.as_ref().is_some_and(AttemptStatus::is_terminal)
            || (status.is_none() && !self.state.attempt_dir(context).exists()))
            && runtime::cgroup_empty(&self.cgroup_path(context))?
        {
            return remove_staging_temps(&self.state.attempt_dir(context));
        }
        Err(BackendError::Conflict(
            "Apptainer cleanup requires terminal evidence and an empty cgroup".to_string(),
        ))
    }
}

pub fn dispatch(mode: &str) -> i32 {
    let Some(path) = std::env::args_os().nth(2).map(PathBuf::from) else {
        return 64;
    };
    let result = match mode {
        "apptainer-supervisor" => runtime::supervisor(&path),
        "payload-launcher" => runtime::launcher(&path),
        _ => return 64,
    };
    if result.is_ok() { 0 } else { 1 }
}

/// Read live rather than cached so the manifest identity can never disagree
/// with the identity `validate_spec` checks at launch.
fn process_identity() -> UserSpec {
    UserSpec {
        uid: rustix::process::geteuid().as_raw(),
        gid: rustix::process::getegid().as_raw(),
    }
}

fn is_root(user: UserSpec) -> bool {
    user.uid == 0 || user.gid == 0
}

/// Apptainer performs no user switch, so a `run_as` other than the process
/// identity would make the manifest lie about who the task ran as.
fn check_identity(run_as: UserSpec, process: UserSpec) -> Result<(), BackendError> {
    if is_root(run_as) {
        return Err(BackendError::InvalidSpec(
            "Apptainer tasks must not run as root".to_string(),
        ));
    }
    if run_as != process {
        return Err(BackendError::InvalidSpec(
            "Apptainer run_as must match the Aruna process user".to_string(),
        ));
    }
    Ok(())
}

fn validate_spec(context: &FenceContext, spec: &TaskSpec) -> Result<(), BackendError> {
    if context.attempt != spec.attempt {
        return Err(BackendError::InvalidSpec(
            "fence and task attempt differ".to_string(),
        ));
    }
    if !digest_pinned(&spec.image) {
        return Err(BackendError::InvalidSpec(
            "task image must be digest-pinned".to_string(),
        ));
    }
    StageLayout::from_spec(spec)?;
    check_identity(spec.security.run_as, process_identity())?;
    match (spec.staging_mode, spec.security.network) {
        (StagingMode::Files, NetworkAccess::Isolated)
        | (StagingMode::DirectS3, NetworkAccess::Open) => {}
        (StagingMode::DirectS3, _) => {
            return Err(BackendError::InvalidSpec(
                "DirectS3 requires open networking for Apptainer".to_string(),
            ));
        }
        (StagingMode::Files, _) => {
            return Err(BackendError::InvalidSpec(
                "Files staging requires isolated networking".to_string(),
            ));
        }
    }
    if spec.resources.disk_bytes.is_some() {
        return Err(BackendError::InvalidSpec(
            "Apptainer cannot enforce a per-attempt disk ceiling".to_string(),
        ));
    }
    if let Some(extension) = spec.resources.backend_extensions.keys().next() {
        return Err(BackendError::InvalidSpec(format!(
            "backend extension `{extension}` is not supported by Apptainer"
        )));
    }
    Ok(())
}

async fn stage_files(directory: &Path, plan: StagePlan) -> Result<Vec<BindMount>, BackendError> {
    let root = directory.join("workspace/root");
    let mut binds = Vec::new();
    for mut entry in plan.entries {
        let host = host_path(&root, &entry.path);
        if let Some(parent) = host.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(io_error)?;
        }
        let mut file = tokio::fs::File::create(&host).await.map_err(io_error)?;
        let mut written = 0u64;
        while let Some(chunk) = entry.stream.next().await {
            let chunk = chunk.map_err(io_error)?;
            written = written.checked_add(chunk.len() as u64).ok_or_else(|| {
                BackendError::InvalidSpec("input byte count overflows".to_string())
            })?;
            if written > entry.size || written > MAX_TRANSFER_BYTES {
                return Err(BackendError::InvalidSpec(format!(
                    "input `{}` exceeds its declared size",
                    entry.path.display()
                )));
            }
            file.write_all(&chunk).await.map_err(io_error)?;
        }
        if written != entry.size {
            return Err(BackendError::InvalidSpec(format!(
                "input `{}` size does not match its declaration",
                entry.path.display()
            )));
        }
        file.sync_all().await.map_err(io_error)?;
        tokio::fs::set_permissions(&host, std::fs::Permissions::from_mode(0o444))
            .await
            .map_err(io_error)?;
        binds.push(BindMount {
            host,
            container: entry.path,
            writable: false,
        });
    }
    for output in plan.layout.output_parents {
        let host = host_path(&root, &output);
        tokio::fs::create_dir_all(&host).await.map_err(io_error)?;
        tokio::fs::set_permissions(&host, std::fs::Permissions::from_mode(0o770))
            .await
            .map_err(io_error)?;
        binds.push(BindMount {
            host,
            container: output,
            writable: true,
        });
    }
    File::open(&root)
        .and_then(|directory| directory.sync_all())
        .map_err(io_error)?;
    Ok(binds)
}

fn command_argv(spec: &TaskSpec, metadata: &OciMetadata) -> Result<Vec<String>, BackendError> {
    let mut argv = spec
        .entrypoint
        .clone()
        .unwrap_or_else(|| metadata.entrypoint.clone());
    if spec.command.is_empty() && spec.entrypoint.is_none() {
        argv.extend(metadata.command.clone());
    } else {
        argv.extend(spec.command.clone());
    }
    if argv.is_empty() {
        return Err(BackendError::InvalidSpec(
            "image and task do not define a command".to_string(),
        ));
    }
    Ok(argv)
}

async fn inspect_metadata(sif: &Path) -> Result<OciMetadata, BackendError> {
    let output = TokioCommand::new("apptainer")
        .args(["inspect", "--json"])
        .arg(sif)
        .output()
        .await
        .map_err(io_error)?;
    if !output.status.success() {
        return Err(BackendError::Api(format!(
            "Apptainer inspect failed with {}",
            output.status
        )));
    }
    parse_metadata(&output.stdout)
}

fn parse_metadata(bytes: &[u8]) -> Result<OciMetadata, BackendError> {
    let value: serde_json::Value = serde_json::from_slice(bytes)
        .map_err(|error| BackendError::Api(format!("decode Apptainer inspect: {error}")))?;
    Ok(OciMetadata {
        entrypoint: find_strings(&value, "Entrypoint").unwrap_or_default(),
        command: find_strings(&value, "Cmd").unwrap_or_default(),
    })
}

fn find_strings(value: &serde_json::Value, key: &str) -> Option<Vec<String>> {
    match value {
        serde_json::Value::Object(object) => {
            if let Some(serde_json::Value::Array(values)) = object.get(key) {
                let strings = values
                    .iter()
                    .map(|value| value.as_str().map(str::to_string))
                    .collect::<Option<Vec<_>>>()?;
                return Some(strings);
            }
            object.values().find_map(|value| find_strings(value, key))
        }
        serde_json::Value::Array(values) => {
            values.iter().find_map(|value| find_strings(value, key))
        }
        _ => None,
    }
}

async fn resolve_digest(
    image: &str,
    deadline: Duration,
    cancel: &CancellationToken,
) -> Result<String, BackendError> {
    let mut command = TokioCommand::new("apptainer");
    command
        .args(["inspect", "--json"])
        .arg(format!("docker://{image}"))
        .kill_on_drop(true);
    let output = tokio::select! {
        _ = cancel.cancelled() => return Err(BackendError::Cancelled),
        result = tokio::time::timeout(deadline, command.output()) => result
            .map_err(|_| BackendError::Timeout("Apptainer image inspect timed out".to_string()))?
            .map_err(io_error)?,
    };
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        if image_missing(&stderr) {
            return Err(BackendError::ImageNotFound(image.to_string()));
        }
        return Err(BackendError::Unavailable(format!(
            "Apptainer image inspect failed with {}: {}",
            output.status,
            stderr.trim()
        )));
    }
    let value: serde_json::Value = serde_json::from_slice(&output.stdout)
        .map_err(|error| BackendError::Api(format!("decode Apptainer inspect: {error}")))?;
    let digest = find_digest(&value).ok_or_else(|| {
        BackendError::Api(format!("image `{image}` did not expose an OCI digest"))
    })?;
    Ok(format!("{}@{digest}", repository_name(image)))
}

fn image_missing(stderr: &str) -> bool {
    let stderr = stderr.to_ascii_lowercase();
    ["image not found", "manifest unknown", "name unknown"]
        .iter()
        .any(|message| stderr.contains(message))
}

fn find_digest(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(value)
            if value.len() == 71
                && value.starts_with("sha256:")
                && value[7..].bytes().all(|byte| byte.is_ascii_hexdigit()) =>
        {
            Some(value.clone())
        }
        serde_json::Value::Object(object) => object.values().find_map(find_digest),
        serde_json::Value::Array(values) => values.iter().find_map(find_digest),
        _ => None,
    }
}

fn repository_name(image: &str) -> &str {
    let without_digest = image.split_once('@').map_or(image, |(name, _)| name);
    let slash = without_digest.rfind('/');
    let colon = without_digest.rfind(':');
    if colon.is_some_and(|colon| slash.is_none_or(|slash| colon > slash)) {
        &without_digest[..colon.unwrap_or(without_digest.len())]
    } else {
        without_digest
    }
}

async fn read_log(
    path: &Path,
    stream: LogStream,
    limits: &LogLimits,
    sink: &dyn LogSink,
) -> Result<(Vec<u8>, u64, bool), BackendError> {
    let mut file = match tokio::fs::File::open(path).await {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok((Vec::new(), 0, false));
        }
        Err(error) => return Err(io_error(error)),
    };
    let mut tail = BoundedTail::new(limits.max_bytes_per_stream);
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer).await.map_err(io_error)?;
        if count == 0 {
            break;
        }
        sink.write(stream, &buffer[..count]);
        tail.push(&buffer[..count]);
    }
    let total = tail.total();
    let truncated = tail.truncated();
    Ok((tail.into_bytes(), total, truncated))
}

async fn stream_file(path: PathBuf, tx: mpsc::Sender<Result<Bytes, BackendError>>) {
    let result = async {
        let mut file = tokio::fs::File::open(path).await.map_err(io_error)?;
        let mut buffer = vec![0u8; 64 * 1024];
        loop {
            let count = file.read(&mut buffer).await.map_err(io_error)?;
            if count == 0 {
                return Ok(());
            }
            if tx
                .send(Ok(Bytes::copy_from_slice(&buffer[..count])))
                .await
                .is_err()
            {
                return Ok(());
            }
        }
    }
    .await;
    if let Err(error) = result {
        let _ = tx.send(Err(error)).await;
    }
}

struct ChannelStream(mpsc::Receiver<Result<Bytes, BackendError>>);

impl Stream for ChannelStream {
    type Item = Result<Bytes, BackendError>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(context)
    }
}

fn validate_control(
    record: Option<ControlRecord>,
    context: &FenceContext,
) -> Result<(), BackendError> {
    let record = record.ok_or_else(|| BackendError::NotFound("control record".to_string()))?;
    if record.attempt_epoch != context.attempt_epoch {
        return Err(BackendError::Conflict("attempt epoch mismatch".to_string()));
    }
    if context.controller_generation < record.highest_generation {
        return Err(BackendError::Fenced);
    }
    Ok(())
}

fn to_status(directory: &Path, record: StatusRecord) -> AttemptStatus {
    AttemptStatus {
        phase: record.phase,
        backend_ref: directory.display().to_string(),
        started_at_ms: record.started_at_ms,
        finished_at_ms: Some(record.finished_at_ms),
    }
}

fn submitted_status(directory: &Path) -> AttemptStatus {
    AttemptStatus {
        phase: AttemptPhase::Submitted,
        backend_ref: directory.display().to_string(),
        started_at_ms: None,
        finished_at_ms: None,
    }
}

fn running_status(directory: &Path) -> AttemptStatus {
    AttemptStatus {
        phase: AttemptPhase::Running,
        backend_ref: directory.display().to_string(),
        started_at_ms: None,
        finished_at_ms: None,
    }
}

fn lost_status(directory: &Path) -> AttemptStatus {
    AttemptStatus {
        phase: AttemptPhase::Failed {
            reason: "lost evidence after recorded launch".to_string(),
        },
        backend_ref: directory.display().to_string(),
        started_at_ms: None,
        finished_at_ms: Some(now_ms()),
    }
}

/// The supervisor writes `status.json` only after reaping the payload, so a live
/// supervisor means the terminal record is still pending rather than lost.
fn settle_status(directory: &Path, pending: AttemptStatus) -> Result<AttemptStatus, BackendError> {
    let supervisor = read_optional::<ProcessRecord>(&directory.join("supervisor.json"))?;
    if supervisor
        .map(|record| runtime::process_live(&record))
        .transpose()?
        .unwrap_or(false)
    {
        return Ok(pending);
    }
    // Re-read: the supervisor may have written the record and exited since.
    if let Some(status) = read_optional::<StatusRecord>(&directory.join("status.json"))? {
        return Ok(to_status(directory, status));
    }
    Ok(lost_status(directory))
}

fn host_path(root: &Path, path: &Path) -> PathBuf {
    root.join(path.strip_prefix("/").unwrap_or(path))
}

fn validate_env_key(key: &str) -> Result<(), BackendError> {
    if key.is_empty()
        || !key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(BackendError::InvalidSpec(format!(
            "invalid environment key `{key}`"
        )));
    }
    Ok(())
}

/// Staging temps are generation-keyed, so a handoff strands foreign generations
/// that only a prefix sweep reclaims.
fn remove_staging_temps(directory: &Path) -> Result<(), BackendError> {
    let (Some(parent), Some(name)) = (directory.parent(), directory.file_name()) else {
        return Ok(());
    };
    let prefix = format!("{}.", name.to_string_lossy());
    let entries = match std::fs::read_dir(parent) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(io_error(error)),
    };
    for entry in entries {
        let entry = entry.map_err(io_error)?;
        let entry_name = entry.file_name();
        let entry_name = entry_name.to_string_lossy();
        if entry_name.starts_with(&prefix) && entry_name.ends_with(".tmp") {
            remove_tree(&entry.path())?;
        }
    }
    Ok(())
}

fn remove_tree(path: &Path) -> Result<(), BackendError> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error(error)),
    }
}

fn remove_cgroup(path: &Path) -> Result<(), BackendError> {
    match std::fs::remove_dir(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error(error)),
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn io_error(error: std::io::Error) -> BackendError {
    BackendError::Api(format!("Apptainer backend: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::compute::{AttemptRef, NOBODY};
    use tempfile::tempdir;

    #[tokio::test]
    async fn absent_cleanup_passes() {
        let root = tempdir().unwrap();
        let backend = ApptainerBackend::with_config(ApptainerConfig {
            state_root: root.path().join("state"),
            sif_cache: root.path().join("cache"),
            cgroup_root: root.path().join("cgroup"),
            stop_grace: Duration::from_secs(1),
            pull_deadline: Duration::from_secs(30),
        })
        .unwrap();
        let context = FenceContext {
            attempt: AttemptRef::new("job", 1),
            attempt_epoch: 1,
            controller_generation: 1,
        };

        backend.cleanup(&context).await.unwrap();
    }

    #[test]
    fn sweeps_generation_temps() {
        // Only this attempt's temps go, whatever generation named them.
        let root = tempdir().unwrap();
        let directory = root.path().join("job-1");
        let foreign = root.path().join("job-1.7.tmp");
        let sibling = root.path().join("job-12.7.tmp");
        for path in [&directory, &foreign, &sibling] {
            std::fs::create_dir_all(path.join("workspace")).unwrap();
        }

        remove_staging_temps(&directory).unwrap();

        assert!(!foreign.exists());
        assert!(directory.exists());
        assert!(sibling.exists());
    }

    #[tokio::test]
    async fn removes_foreign_temps() {
        // A temp stranded by an earlier generation must not survive cleanup.
        let root = tempdir().unwrap();
        let backend = test_backend(root.path());
        let context = FenceContext {
            attempt: AttemptRef::new("job", 9),
            attempt_epoch: 1,
            controller_generation: 3,
        };
        let foreign = backend.state.attempt_dir(&context).with_extension("1.tmp");
        std::fs::create_dir_all(foreign.join("workspace")).unwrap();

        backend.cleanup(&context).await.unwrap();

        assert!(!foreign.exists());
    }

    fn test_backend(root: &Path) -> ApptainerBackend {
        ApptainerBackend::with_config(ApptainerConfig {
            state_root: root.join("state"),
            sif_cache: root.join("cache"),
            cgroup_root: root.join("cgroup"),
            stop_grace: Duration::from_secs(1),
            pull_deadline: Duration::from_secs(30),
        })
        .unwrap()
    }

    fn live_record() -> ProcessRecord {
        runtime::process_record(std::process::id()).unwrap()
    }

    /// Own pid with foreign start ticks: the pidfd opens but identity mismatches.
    fn dead_record() -> ProcessRecord {
        ProcessRecord {
            pid: std::process::id(),
            start_ticks: u64::MAX,
        }
    }

    fn reaped_attempt(backend: &ApptainerBackend, context: &FenceContext) -> PathBuf {
        let directory = backend.state.attempt_dir(context);
        std::fs::create_dir_all(&directory).unwrap();
        write_json(
            &directory.join("payload.json"),
            &PayloadRecord {
                process: dead_record(),
                cgroup: backend.cgroup_path(context),
            },
        )
        .unwrap();
        directory
    }

    #[tokio::test]
    async fn pending_stays_running() {
        // Payload reaped and cgroup drained, but the supervisor has not written
        // status.json yet: a success must never latch as terminally Failed.
        let root = tempdir().unwrap();
        let backend = test_backend(root.path());
        let context = FenceContext {
            attempt: AttemptRef::new("job", 6),
            attempt_epoch: 1,
            controller_generation: 1,
        };
        drop(backend.state.control(&context).unwrap());
        let directory = reaped_attempt(&backend, &context);
        write_json(&directory.join("supervisor.json"), &live_record()).unwrap();

        let status = backend.attempt_status(&context).unwrap();

        assert!(!status.is_terminal(), "got {status:?}");
    }

    #[tokio::test]
    async fn lost_supervisor_fails() {
        // Same evidence, but with the supervisor gone the record can never land.
        let root = tempdir().unwrap();
        let backend = test_backend(root.path());
        let context = FenceContext {
            attempt: AttemptRef::new("job", 7),
            attempt_epoch: 1,
            controller_generation: 1,
        };
        drop(backend.state.control(&context).unwrap());
        let directory = reaped_attempt(&backend, &context);
        write_json(&directory.join("supervisor.json"), &dead_record()).unwrap();

        let status = backend.attempt_status(&context).unwrap();

        assert!(matches!(status.phase, AttemptPhase::Failed { .. }));
    }

    #[test]
    fn reads_late_status() {
        // The record may land between the caller's first read and this check.
        let root = tempdir().unwrap();
        write_json(&root.path().join("supervisor.json"), &dead_record()).unwrap();
        write_json(
            &root.path().join("status.json"),
            &StatusRecord {
                phase: AttemptPhase::Exited { code: 0 },
                started_at_ms: Some(1),
                finished_at_ms: 2,
            },
        )
        .unwrap();

        let status = settle_status(root.path(), running_status(root.path())).unwrap();

        assert!(matches!(status.phase, AttemptPhase::Exited { code: 0 }));
    }

    #[tokio::test]
    async fn failed_delete_unsealed() {
        let root = tempdir().unwrap();
        let backend = ApptainerBackend::with_config(ApptainerConfig {
            state_root: root.path().join("state"),
            sif_cache: root.path().join("cache"),
            cgroup_root: root.path().join("cgroup"),
            stop_grace: Duration::from_secs(1),
            pull_deadline: Duration::from_secs(30),
        })
        .unwrap();
        let context = FenceContext {
            attempt: AttemptRef::new("job", 2),
            attempt_epoch: 1,
            controller_generation: 1,
        };
        let cgroup = backend.cgroup_path(&context);
        std::fs::create_dir_all(&cgroup).unwrap();
        std::fs::write(cgroup.join("cgroup.procs"), []).unwrap();

        backend
            .tombstone(&context, &TombstoneSpec { terminal_ref: None })
            .await
            .unwrap_err();
        assert!(
            backend
                .state
                .read(&context)
                .unwrap()
                .unwrap()
                .tombstone_ref
                .is_none()
        );
    }

    fn identity_spec(context: &FenceContext) -> TaskSpec {
        TaskSpec::new(
            context.attempt.clone(),
            "repo@sha256:0000000000000000000000000000000000000000000000000000000000000000",
        )
    }

    fn identity_context() -> FenceContext {
        FenceContext {
            attempt: AttemptRef::new("job", 3),
            attempt_epoch: 1,
            controller_generation: 1,
        }
    }

    #[test]
    fn accepts_matching_identity() {
        // Identities are injected: the check must hold whatever uid the runner has.
        let user = UserSpec { uid: 1234, gid: 56 };

        assert!(check_identity(user, user).is_ok());
    }

    #[test]
    fn rejects_foreign_identity() {
        let process = UserSpec { uid: 1234, gid: 56 };
        let foreign = UserSpec { uid: 1235, gid: 56 };

        assert!(check_identity(foreign, process).is_err());
        assert!(check_identity(NOBODY, process).is_err());
    }

    #[test]
    fn rejects_root_identity() {
        // Root must lose even when it matches a root service, which is health's gate.
        let root = UserSpec { uid: 0, gid: 0 };
        let root_group = UserSpec { uid: 1234, gid: 0 };

        assert!(check_identity(root, root).is_err());
        assert!(check_identity(root_group, root_group).is_err());
        assert!(is_root(root) && is_root(root_group));
        assert!(!is_root(UserSpec { uid: 1234, gid: 56 }));
    }

    #[test]
    fn spec_uses_identity() {
        // Wiring check: a foreign run_as is rejected under any runner uid.
        let context = identity_context();
        let mut spec = identity_spec(&context);
        spec.security.run_as = process_identity();
        assert_eq!(
            validate_spec(&context, &spec).is_ok(),
            !is_root(spec.security.run_as)
        );

        spec.security.run_as.uid ^= 1;
        assert!(validate_spec(&context, &spec).is_err());
    }

    #[test]
    fn classifies_registry_failure() {
        assert!(image_missing("MANIFEST_UNKNOWN: manifest unknown"));
        assert!(!image_missing("dial tcp: connection timed out"));
    }

    #[tokio::test]
    async fn pull_observes_cancel() {
        let root = tempdir().unwrap();
        let backend = ApptainerBackend::with_config(ApptainerConfig {
            state_root: root.path().join("state"),
            sif_cache: root.path().join("cache"),
            cgroup_root: root.path().join("cgroup"),
            stop_grace: Duration::from_secs(1),
            pull_deadline: Duration::from_secs(30),
        })
        .unwrap();
        let cancel = CancellationToken::new();
        cancel.cancel();

        let error = backend
            .ensure_sif(
                "repo@sha256:0000000000000000000000000000000000000000000000000000000000000000",
                &cancel,
            )
            .await
            .unwrap_err();
        assert!(matches!(error, BackendError::Cancelled));
    }

    #[test]
    fn combines_oci_command() {
        let mut spec = TaskSpec::new(
            AttemptRef::new("job", 0),
            "repo@sha256:0000000000000000000000000000000000000000000000000000000000000000",
        );
        spec.command = vec!["argument".to_string()];
        let metadata = OciMetadata {
            entrypoint: vec!["/bin/tool".to_string()],
            command: vec!["default".to_string()],
        };
        assert_eq!(
            command_argv(&spec, &metadata).unwrap(),
            vec!["/bin/tool", "argument"]
        );
    }

    #[test]
    fn drops_oci_command() {
        let mut spec = TaskSpec::new(
            AttemptRef::new("job", 0),
            "repo@sha256:0000000000000000000000000000000000000000000000000000000000000000",
        );
        spec.entrypoint = Some(vec!["/bin/tool".to_string()]);
        let metadata = OciMetadata {
            entrypoint: Vec::new(),
            command: vec!["default".to_string()],
        };

        assert_eq!(command_argv(&spec, &metadata).unwrap(), vec!["/bin/tool"]);
    }

    #[test]
    fn parses_oci_metadata() {
        let metadata = parse_metadata(
            br#"{"data":{"attributes":{"config":{"Entrypoint":["/bin/tool"],"Cmd":["run"]}}}}"#,
        )
        .unwrap();
        assert_eq!(metadata.entrypoint, ["/bin/tool"]);
        assert_eq!(metadata.command, ["run"]);
    }

    #[test]
    fn strips_image_tag() {
        assert_eq!(
            repository_name("registry:5000/team/image:tag"),
            "registry:5000/team/image"
        );
        assert_eq!(repository_name("alpine:3.24"), "alpine");
    }
}
