//! Runs each attempt as one Slurm job that starts its Apptainer container on a compute node.
//! The state root is one path the controller and every compute node share.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

use aruna_core::compute::{
    AdoptableEvidence, ArtifactEvidence, AttemptPhase, AttemptStatus, BackendError, CancelEvidence,
    ExecutorKind, FenceContext, GrantedResources, LogLimits, LogTails, OutputMatcher,
    ReconcileEvidence, ResumePoint, TaskOutput, TaskSpec, TombstoneEvidence, TombstoneSpec,
    UserSpec, literal_prefix, normalize_container_path,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::state::{AttemptRecord, BindMount, StateRoot, read_json, read_optional, write_json};
use super::{
    declared_outputs, host_path, io_error, list_workspace, process_identity, read_log,
    remove_staging_temps, remove_tree, stage_files, stream_file, validate_control, validate_spec,
};
use crate::executor::channel::ChannelStream;
use crate::executor::config::SlurmConfig;
use crate::executor::staging::StagePlan;
use crate::executor::{BackendCaps, ExecutorBackend, WorkerSite, digest_pinned, enforced_limit};

#[path = "scheduler.rs"]
mod scheduler;

use scheduler::{Scheduler, env_script, job_phase, job_script, write_private};

/// Written before `sbatch`, so an interrupted submit is found again by the job's name.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct SubmitRecord {
    job_name: String,
    /// Local time `sacct -S` searches from.
    since: String,
}

/// The job id `sbatch` returned.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct JobRecord {
    job_id: String,
}

enum Lookup {
    Found(String),
    Missing,
    Multiple(Vec<String>),
}

pub struct SlurmBackend {
    config: SlurmConfig,
    state: StateRoot,
}

impl SlurmBackend {
    pub fn with_config(config: SlurmConfig) -> Result<Self, BackendError> {
        let state = StateRoot::open(&config.state_root)?;
        Ok(Self { config, state })
    }

    /// The recorded job, else the jobs carrying the attempt's name after an interrupted submit.
    async fn lookup(&self, context: &FenceContext) -> Result<Lookup, BackendError> {
        let directory = self.state.attempt_dir(context);
        if let Some(job) = read_optional::<JobRecord>(&directory.join("slurm.json"))? {
            return Ok(Lookup::Found(job.job_id));
        }
        let Some(submit) = read_optional::<SubmitRecord>(&directory.join("submit.json"))? else {
            return Ok(Lookup::Missing);
        };
        let scheduler = Scheduler::new(&self.config);
        let mut ids = scheduler.find(&submit.job_name, &submit.since).await?;
        Ok(match ids.len() {
            0 => Lookup::Missing,
            1 => Lookup::Found(ids.remove(0)),
            _ => Lookup::Multiple(ids),
        })
    }

    async fn attempt_status(&self, context: &FenceContext) -> Result<AttemptStatus, BackendError> {
        let control = self.state.read(context)?;
        let cancel_requested = control.as_ref().is_some_and(|record| record.cancel);
        validate_control(control, context)?;
        let id = match self.lookup(context).await? {
            Lookup::Found(id) => id,
            Lookup::Missing => return Err(BackendError::NotFound(job_name(context))),
            Lookup::Multiple(ids) => return Err(several_jobs(&ids)),
        };
        let backend_ref = format!("slurm:{id}");
        let Some(job) = Scheduler::new(&self.config).state(&id).await? else {
            return Ok(AttemptStatus {
                phase: AttemptPhase::SystemError {
                    reason: "Slurm has no record of the job".to_string(),
                },
                backend_ref,
                started_at_ms: None,
                finished_at_ms: Some(crate::executor::now_ms()),
                detail: None,
            });
        };
        let phase = job_phase(&job, cancel_requested);
        let finished_at_ms = (phase.is_terminal())
            .then(|| (job.finished_at_ms).unwrap_or_else(crate::executor::now_ms));
        Ok(AttemptStatus {
            phase,
            backend_ref,
            started_at_ms: job.started_at_ms,
            finished_at_ms,
            detail: None,
        })
    }

    /// The `sbatch` options that enforce the attempt's memory, CPU and walltime.
    fn job_limits(&self, spec: &TaskSpec) -> Result<Vec<String>, BackendError> {
        let granted = self.granted(spec)?;
        let memory = granted.ram_bytes.unwrap_or_default().div_ceil(1024 * 1024);
        let cores = granted.nano_cpus.unwrap_or_default() / 1_000_000_000;
        let mut limits = vec![
            format!("--mem={memory}M"),
            format!("--cpus-per-task={cores}"),
        ];
        if let Some(walltime) = spec.resources.max_walltime {
            let seconds = walltime.as_secs().max(1);
            let (days, rest) = (seconds / 86_400, seconds % 86_400);
            let (hours, minutes, seconds) = (rest / 3600, rest % 3600 / 60, rest % 60);
            limits.push(format!(
                "--time={days}-{hours:02}:{minutes:02}:{seconds:02}"
            ));
        }
        Ok(limits)
    }

    /// Stages the inputs and writes the job script under a temporary name, then moves the
    /// attempt directory into place.
    async fn prepare_attempt(
        &self,
        context: &FenceContext,
        spec: &TaskSpec,
    ) -> Result<PathBuf, BackendError> {
        let plan = StagePlan::from_spec(spec)?;
        let directory = self.state.attempt_dir(context);
        let name = (directory.file_name())
            .map(|name| name.to_string_lossy())
            .unwrap_or_default();
        let generation = context.controller_generation;
        let temp = directory.with_file_name(format!("{name}.{generation}.tmp"));
        remove_staging_temps(&directory)?;
        std::fs::create_dir_all(temp.join("workspace/root")).map_err(io_error)?;
        std::fs::create_dir_all(temp.join("logs")).map_err(io_error)?;
        let layout_digest = plan.layout.digest();
        let binds = stage_files(&temp, plan).await?;
        // Binds name the temporary directory; the job sees the final one.
        let binds: Vec<BindMount> = (binds.into_iter())
            .map(|bind| BindMount {
                host: directory.join(bind.host.strip_prefix(&temp).unwrap_or(&bind.host)),
                ..bind
            })
            .collect();
        write_private(&temp.join("env.sh"), &env_script(spec)?)?;
        let script = job_script(&self.config.apptainer, spec, &binds, &directory);
        std::fs::write(temp.join("job.sh"), script).map_err(io_error)?;
        let mode = std::fs::Permissions::from_mode(0o700);
        std::fs::set_permissions(temp.join("job.sh"), mode).map_err(io_error)?;
        let record = AttemptRecord {
            attempt_epoch: context.attempt_epoch,
            pinned_image: spec.image.clone(),
            layout_digest,
            output_paths: spec.output_paths.clone(),
        };
        write_json(&temp.join("attempt.json"), &record)?;
        // A minute of slack keeps a job submitted around midnight or a clock step findable.
        let since = chrono::Local::now() - chrono::Duration::minutes(1);
        let submit = SubmitRecord {
            job_name: job_name(context),
            since: since.format("%Y-%m-%dT%H:%M:%S").to_string(),
        };
        write_json(&temp.join("submit.json"), &submit)?;
        remove_tree(&directory)?;
        std::fs::rename(&temp, &directory).map_err(io_error)?;
        Ok(directory)
    }
}

/// A status, or `None` for an attempt that was never submitted.
fn existing(
    result: Result<AttemptStatus, BackendError>,
) -> Result<Option<AttemptStatus>, BackendError> {
    match result {
        Ok(status) => Ok(Some(status)),
        Err(BackendError::NotFound(_)) => Ok(None),
        Err(error) => Err(error),
    }
}

/// The deterministic Slurm job name of one attempt epoch.
fn job_name(context: &FenceContext) -> String {
    format!(
        "{}-e{}",
        context.attempt.external_name(),
        context.attempt_epoch
    )
}

fn several_jobs(ids: &[String]) -> BackendError {
    BackendError::Conflict(format!(
        "several Slurm jobs carry this attempt's name: {}",
        ids.join(", ")
    ))
}

#[async_trait]
impl ExecutorBackend for SlurmBackend {
    fn kind(&self) -> ExecutorKind {
        ExecutorKind::Slurm
    }

    fn capabilities(&self) -> BackendCaps {
        let location = self.config.execution_location.trim();
        BackendCaps {
            file_staging: true,
            direct_s3: true,
            worker_site: (!location.is_empty()).then(|| WorkerSite {
                location: location.to_string(),
                labels: self.config.execution_labels.clone(),
            }),
            limits: self.config.envelope,
            ..BackendCaps::default()
        }
    }

    fn granted(&self, spec: &TaskSpec) -> Result<GrantedResources, BackendError> {
        let config = &self.config;
        let memory = enforced_limit(spec.resources.ram_bytes, config.default_mem_bytes, "memory")?;
        let cores = enforced_limit(
            spec.resources.cpu_cores.map(u64::from),
            config.default_cpu_cores.map(u64::from),
            "cpu",
        )?;
        Ok(GrantedResources {
            nano_cpus: Some(cores.min(u64::from(u32::MAX)) * 1_000_000_000),
            ram_bytes: Some(memory),
            disk_bytes: None,
            workspace_bytes: None,
            walltime_ms: (spec.resources.max_walltime).map(aruna_core::telemetry::duration_ms),
        })
    }

    /// Slurm runs the job as the user that submits it, which is this service.
    fn run_identity(&self) -> UserSpec {
        process_identity()
    }

    async fn health(&self) -> Result<(), BackendError> {
        Scheduler::new(&self.config).version().await?;
        self.state.verify()
    }

    /// Compute nodes pull the image themselves, so only a pinned digest is accepted.
    async fn resolve_image(
        &self,
        image: &str,
        _cancel: &CancellationToken,
    ) -> Result<String, BackendError> {
        match digest_pinned(image) {
            true => Ok(image.to_string()),
            false => Err(BackendError::InvalidSpec(
                "Slurm tasks need a digest-pinned image".to_string(),
            )),
        }
    }

    async fn fence(&self, context: &FenceContext) -> Result<(), BackendError> {
        (context.attempt.validate()).map_err(BackendError::InvalidSpec)?;
        self.state.control(context).map(|_| ())
    }

    async fn submit(
        &self,
        context: &FenceContext,
        spec: &TaskSpec,
        cancel: &CancellationToken,
    ) -> Result<AttemptStatus, BackendError> {
        validate_spec(context, spec)?;
        if spec.session {
            return Err(BackendError::InvalidSpec(
                "Slurm runs no interactive sessions".to_string(),
            ));
        }
        let guard = self.state.control(context)?;
        if guard.tombstone().is_some() {
            return Err(BackendError::Conflict("attempt is tombstoned".to_string()));
        }
        match self.lookup(context).await? {
            Lookup::Found(_) => return self.attempt_status(context).await,
            Lookup::Multiple(ids) => return Err(several_jobs(&ids)),
            Lookup::Missing => {}
        }
        if cancel.is_cancelled() {
            return Err(BackendError::Cancelled);
        }
        let limits = self.job_limits(spec)?;
        let directory = self.prepare_attempt(context, spec).await?;
        let scheduler = Scheduler::new(&self.config);
        let job_id = scheduler
            .submit(&job_name(context), &directory, &limits)
            .await?;
        let backend_ref = format!("slurm:{job_id}");
        write_json(&directory.join("slurm.json"), &JobRecord { job_id })?;
        drop(guard);
        Ok(AttemptStatus {
            phase: AttemptPhase::Submitted,
            backend_ref,
            started_at_ms: None,
            finished_at_ms: None,
            detail: None,
        })
    }

    async fn status(&self, context: &FenceContext) -> Result<AttemptStatus, BackendError> {
        self.attempt_status(context).await
    }

    async fn cancel(&self, context: &FenceContext) -> Result<CancelEvidence, BackendError> {
        let mut guard = self.state.control(context)?;
        let status = match existing(self.attempt_status(context).await)? {
            Some(status) if status.is_terminal() => return Ok(CancelEvidence::Stopped(status)),
            Some(status) => status,
            None => return Ok(CancelEvidence::AlreadyGone),
        };
        guard.mark_cancel()?;
        let id = status.backend_ref.trim_start_matches("slurm:");
        Scheduler::new(&self.config).cancel(id).await?;
        Ok(CancelEvidence::Requested)
    }

    async fn fetch_logs(
        &self,
        context: &FenceContext,
        limits: &LogLimits,
    ) -> Result<LogTails, BackendError> {
        validate_control(self.state.read(context)?, context)?;
        let logs = self.state.attempt_dir(context).join("logs");
        let stdout = read_log(&logs.join("stdout"), limits).await?;
        let stderr = read_log(&logs.join("stderr"), limits).await?;
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
        let root = self.terminal_root(context).await?;
        let directory = self.state.attempt_dir(context);
        let attempt: AttemptRecord = read_json(&directory.join("attempt.json"))?;
        if !declared_outputs(&attempt)?.is_match(path) {
            return Err(BackendError::InvalidSpec(
                "output path was not declared".to_string(),
            ));
        }
        let normalized = normalize_container_path(path).map_err(BackendError::InvalidSpec)?;
        let candidate = host_path(&root, &normalized)
            .canonicalize()
            .map_err(io_error)?;
        if !candidate.starts_with(&root) || !candidate.is_file() {
            return Err(BackendError::InvalidSpec(
                "output is not a regular workspace file".to_string(),
            ));
        }
        let size = candidate.metadata().map_err(io_error)?.len();
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(stream_file(candidate, tx));
        Ok(TaskOutput {
            size,
            chunks: Box::pin(ChannelStream(rx)),
        })
    }

    async fn list_outputs(
        &self,
        context: &FenceContext,
        pattern: &str,
    ) -> Result<Vec<String>, BackendError> {
        let root = self.terminal_root(context).await?;
        let directory = self.state.attempt_dir(context);
        let attempt: AttemptRecord = read_json(&directory.join("attempt.json"))?;
        if !attempt.output_paths.iter().any(|output| output == pattern) {
            return Err(BackendError::InvalidSpec(
                "output path was not declared".to_string(),
            ));
        }
        let glob = OutputMatcher::new([pattern]).map_err(BackendError::InvalidSpec)?;
        let prefix = literal_prefix(pattern).map_err(BackendError::InvalidSpec)?;
        list_workspace(&root, &host_path(&root, &prefix), &glob)
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
        match self.attempt_status(context).await {
            Ok(status) => ReconcileEvidence::Adoptable(AdoptableEvidence {
                status,
                resume: ResumePoint::Observe,
            }),
            Err(BackendError::NotFound(_)) => ReconcileEvidence::Absent,
            Err(BackendError::Conflict(_)) => ReconcileEvidence::Unadoptable(ArtifactEvidence {
                artifact_kind: "slurm-job".to_string(),
                backend_ref: Some(job_name(context)),
                observed_epoch: Some(context.attempt_epoch),
                observed_generation: None,
                exact_identity: true,
                multiple: true,
                foreign: false,
            }),
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
        if let Some(status) = existing(self.attempt_status(context).await)?
            && !status.is_terminal()
        {
            return Err(BackendError::Conflict(
                "cannot tombstone a live Slurm job".to_string(),
            ));
        }
        let reference = self.state.control_dir(context).join("tombstone.json");
        write_json(
            &reference,
            &BTreeMap::from([
                ("attempt_epoch", context.attempt_epoch.to_string()),
                ("job_name", job_name(context)),
            ]),
        )?;
        remove_tree(&self.state.attempt_dir(context))?;
        guard.store(reference.display().to_string())
    }

    async fn cleanup(&self, context: &FenceContext) -> Result<(), BackendError> {
        let _guard = self.state.control(context)?;
        let directory = self.state.attempt_dir(context);
        let status = existing(self.attempt_status(context).await)?;
        if status.as_ref().is_some_and(AttemptStatus::is_terminal)
            || (status.is_none() && !directory.join("submit.json").exists())
        {
            remove_staging_temps(&directory)?;
            // Staged inputs and outputs are plaintext; logs and job records stay as evidence.
            return remove_tree(&directory.join("workspace"));
        }
        Err(BackendError::Conflict(
            "Slurm cleanup requires terminal evidence".to_string(),
        ))
    }
}

impl SlurmBackend {
    /// The workspace root of a terminal attempt.
    async fn terminal_root(&self, context: &FenceContext) -> Result<PathBuf, BackendError> {
        if !self.attempt_status(context).await?.is_terminal() {
            return Err(BackendError::InvalidSpec(
                "attempt is not terminal".to_string(),
            ));
        }
        let root = self.state.attempt_dir(context).join("workspace/root");
        root.canonicalize().map_err(io_error)
    }
}

#[cfg(test)]
#[path = "slurm_tests.rs"]
mod tests;
