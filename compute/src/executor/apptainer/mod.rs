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
    TombstoneSpec, normalize_container_path,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::config::ApptainerConfig;
use super::logs::{BoundedTail, LogSink};
use super::staging::{StageLayout, StagePlan};
use super::{ExecutorBackend, digest_pinned};

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
            return Ok(lost_status(&directory));
        }
        if let Some(supervisor) =
            read_optional::<ProcessRecord>(&directory.join("supervisor.json"))?
        {
            if runtime::process_live(&supervisor)? {
                return Ok(submitted_status(&directory));
            }
            return Ok(submitted_status(&directory));
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
        remove_tree(&temp)?;
        std::fs::create_dir_all(temp.join("workspace/root")).map_err(io_error)?;
        std::fs::create_dir_all(temp.join("logs")).map_err(io_error)?;
        let binds = stage_files(&temp, plan).await?;
        let argv = command_argv(spec, &metadata)?;
        let record = AttemptRecord {
            attempt_epoch: context.attempt_epoch,
            pinned_image: spec.image.clone(),
            layout_digest: layout_digest(spec)?,
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

    fn spawn_supervisor(
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
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn ensure_sif(&self, image: &str) -> Result<(PathBuf, OciMetadata), BackendError> {
        let digest = image
            .rsplit_once("@sha256:")
            .map(|(_, digest)| digest)
            .ok_or_else(|| BackendError::InvalidSpec("image must be digest-pinned".to_string()))?;
        let sif = self.config.sif_cache.join(format!("{digest}.sif"));
        let metadata_path = self.config.sif_cache.join(format!("{digest}.json"));
        let lock = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(self.config.sif_cache.join(format!("{digest}.lock")))
            .map_err(io_error)?;
        lock.lock().map_err(io_error)?;
        if sif.exists() && metadata_path.exists() {
            return Ok((sif, read_json(&metadata_path)?));
        }
        let temp = self.config.sif_cache.join(format!("{digest}.sif.tmp"));
        let status = Command::new("apptainer")
            .args(["pull", "--disable-cache", "--force"])
            .arg(&temp)
            .arg(format!("docker://{image}"))
            .status()
            .map_err(io_error)?;
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
        let metadata = inspect_metadata(&sif)?;
        write_json(&metadata_path, &metadata)?;
        Ok((sif, metadata))
    }
}

#[async_trait]
impl ExecutorBackend for ApptainerBackend {
    fn kind(&self) -> ExecutorKind {
        ExecutorKind::Apptainer
    }

    async fn health(&self) -> Result<(), BackendError> {
        let output = Command::new("apptainer")
            .arg("version")
            .output()
            .map_err(io_error)?;
        if !output.status.success() || output.stdout.is_empty() {
            return Err(BackendError::Unavailable(
                "Apptainer version check failed".to_string(),
            ));
        }
        if rustix::process::geteuid().as_raw() == 0 {
            return Err(BackendError::Unavailable(
                "Apptainer executor must run as a non-root user".to_string(),
            ));
        }
        self.state.verify()?;
        runtime::probe_cgroup(&self.config.cgroup_root)
    }

    async fn resolve_image(&self, image: &str) -> Result<String, BackendError> {
        if digest_pinned(image) {
            Ok(image.to_string())
        } else {
            resolve_digest(image)
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
        let _guard = self.state.control(context)?;
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
        let (sif, metadata) = self.ensure_sif(&spec.image)?;
        let directory = self.prepare_attempt(context, spec, sif, metadata).await?;
        self.spawn_supervisor(&directory, spec, cancel)
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
            Some(status) if status.is_terminal() => return Ok(CancelEvidence::Stopped(status)),
            Some(status) => status,
            None => return Ok(CancelEvidence::AlreadyGone),
        };
        guard.mark_cancel()?;
        let directory = self.state.attempt_dir(context);
        let payload = read_optional::<PayloadRecord>(&directory.join("payload.json"))?;
        runtime::terminate(
            &self.cgroup_path(context),
            payload.as_ref(),
            self.config.stop_grace,
        )?;
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
        let evidence = guard.seal(reference)?;
        remove_tree(&directory)?;
        remove_cgroup(&self.cgroup_path(context))?;
        Ok(evidence)
    }

    async fn cleanup(&self, context: &FenceContext) -> Result<(), BackendError> {
        let _guard = self.state.control(context)?;
        if let Some(status) = self.existing_status(context)?
            && status.is_terminal()
            && runtime::cgroup_empty(&self.cgroup_path(context))?
        {
            return Ok(());
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
    if rustix::process::geteuid().as_raw() != spec.security.run_as.uid
        || rustix::process::getegid().as_raw() != spec.security.run_as.gid
    {
        return Err(BackendError::InvalidSpec(
            "Apptainer run_as must match the Aruna process user".to_string(),
        ));
    }
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
    if spec.command.is_empty() {
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

fn inspect_metadata(sif: &Path) -> Result<OciMetadata, BackendError> {
    let output = Command::new("apptainer")
        .args(["inspect", "--json"])
        .arg(sif)
        .output()
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

fn resolve_digest(image: &str) -> Result<String, BackendError> {
    let output = Command::new("apptainer")
        .args(["inspect", "--json"])
        .arg(format!("docker://{image}"))
        .output()
        .map_err(io_error)?;
    if !output.status.success() {
        return Err(BackendError::ImageNotFound(image.to_string()));
    }
    let value: serde_json::Value = serde_json::from_slice(&output.stdout)
        .map_err(|error| BackendError::Api(format!("decode Apptainer inspect: {error}")))?;
    let digest = find_digest(&value).ok_or_else(|| {
        BackendError::Api(format!("image `{image}` did not expose an OCI digest"))
    })?;
    Ok(format!("{}@{digest}", repository_name(image)))
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

fn layout_digest(spec: &TaskSpec) -> Result<String, BackendError> {
    let layout = StageLayout::from_spec(spec)?;
    let mut rows = layout
        .files
        .iter()
        .map(|file| {
            format!(
                "i\0{}\0{}\0{}",
                file.path.display(),
                file.size,
                file.workspace_key
            )
        })
        .collect::<Vec<_>>();
    rows.extend(
        layout
            .output_parents
            .iter()
            .map(|path| format!("o\0{}", path.display())),
    );
    rows.sort();
    let mut hasher = blake3::Hasher::new();
    hasher.update(match layout.mode {
        StagingMode::Files => b"files",
        StagingMode::DirectS3 => b"direct-s3",
    });
    for row in rows {
        hasher.update(row.as_bytes());
        hasher.update(&[0]);
    }
    Ok(hasher.finalize().to_hex().to_string())
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
    use aruna_core::compute::AttemptRef;

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
