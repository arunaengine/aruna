//! Calls the Slurm command line tools, reads their machine-readable output and writes the job
//! script they run.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Output;

use aruna_core::compute::{AttemptPhase, BackendError, NetworkAccess, TaskSpec};
use chrono::{Local, NaiveDateTime, TimeZone};
use tokio::process::Command;

use crate::executor::apptainer::state::BindMount;
use crate::executor::apptainer::{io_error, validate_env_key};
use crate::executor::config::SlurmConfig;

/// One Slurm job as `squeue` or `sacct` reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct JobState {
    pub(super) state: String,
    pub(super) exit_code: Option<i32>,
    pub(super) signal: Option<i32>,
    pub(super) started_at_ms: Option<u64>,
    pub(super) finished_at_ms: Option<u64>,
}

pub(super) struct Scheduler<'a> {
    config: &'a SlurmConfig,
}

impl<'a> Scheduler<'a> {
    pub(super) fn new(config: &'a SlurmConfig) -> Self {
        Self { config }
    }

    /// Checks that the scheduler answers at all.
    pub(super) async fn version(&self) -> Result<(), BackendError> {
        self.run(&self.config.sbatch, ["--version".to_string()])
            .await
            .map(|_| ())
    }

    /// Submits `script` under `name` and returns the job id.
    pub(super) async fn submit(
        &self,
        name: &str,
        directory: &Path,
        limits: &[String],
    ) -> Result<String, BackendError> {
        let logs = directory.join("logs");
        let mut args = vec![
            "--parsable".to_string(),
            format!("--job-name={name}"),
            format!("--chdir={}", directory.display()),
            format!("--output={}", logs.join("stdout").display()),
            format!("--error={}", logs.join("stderr").display()),
            "--export=NONE".to_string(),
        ];
        args.extend(limits.iter().cloned());
        if let Some(partition) = &self.config.partition {
            args.push(format!("--partition={partition}"));
        }
        if let Some(account) = &self.config.account {
            args.push(format!("--account={account}"));
        }
        args.push(directory.join("job.sh").display().to_string());
        let output = self.run(&self.config.sbatch, args).await?;
        // `--parsable` prints `id` or `id;cluster`.
        let text = String::from_utf8_lossy(&output.stdout);
        let id = text.trim().split(';').next().unwrap_or_default();
        if id.is_empty() || !id.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(BackendError::Api(format!(
                "sbatch printed no job id: {}",
                text.trim()
            )));
        }
        Ok(id.to_string())
    }

    /// The ids of every job named `name`: queued ones, then accounted ones since `since`.
    pub(super) async fn find(&self, name: &str, since: &str) -> Result<Vec<String>, BackendError> {
        let args = ["-h", "-o", "%i"].map(String::from);
        let queued = self
            .run(
                &self.config.squeue,
                [&args[..], &[format!("--name={name}")]].concat(),
            )
            .await?;
        let mut ids = lines(&queued).collect::<Vec<_>>();
        let args = ["-n", "-P", "-X", "-o", "JobID", "-S", since].map(String::from);
        let accounted = self
            .run(
                &self.config.sacct,
                [&args[..], &[format!("--name={name}")]].concat(),
            )
            .await?;
        for id in lines(&accounted) {
            if !ids.contains(&id) {
                ids.push(id);
            }
        }
        ids.sort();
        Ok(ids)
    }

    /// The state of job `id`, from the queue while it is there and from accounting after.
    pub(super) async fn state(&self, id: &str) -> Result<Option<JobState>, BackendError> {
        let args = ["-h", "-o", "%T", "-j", id].map(String::from);
        // An id the queue no longer holds makes squeue fail, so only its output counts.
        let queued = self.output(&self.config.squeue, args).await?;
        if let Some(state) = lines(&queued).next() {
            return Ok(Some(JobState {
                state,
                exit_code: None,
                signal: None,
                started_at_ms: None,
                finished_at_ms: None,
            }));
        }
        let args = ["-n", "-P", "-X", "-o", "State,ExitCode,Start,End", "-j", id];
        let accounted = self.run(&self.config.sacct, args.map(String::from)).await?;
        Ok(lines(&accounted)
            .next()
            .as_deref()
            .and_then(parse_accounted))
    }

    pub(super) async fn cancel(&self, id: &str) -> Result<(), BackendError> {
        self.run(&self.config.scancel, [id.to_string()])
            .await
            .map(|_| ())
    }

    async fn output(
        &self,
        program: &Path,
        args: impl IntoIterator<Item = String>,
    ) -> Result<Output, BackendError> {
        let call = Command::new(program).args(args).kill_on_drop(true).output();
        tokio::time::timeout(self.config.command_timeout, call)
            .await
            .map_err(|_| {
                BackendError::Timeout(format!("{} did not answer in time", program.display()))
            })?
            .map_err(|error| {
                BackendError::Unavailable(format!("cannot run {}: {error}", program.display()))
            })
    }

    async fn run(
        &self,
        program: &Path,
        args: impl IntoIterator<Item = String>,
    ) -> Result<Output, BackendError> {
        let output = self.output(program, args).await?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(BackendError::Unavailable(format!(
                "{} failed: {}",
                program.display(),
                stderr.trim()
            )));
        }
        Ok(output)
    }
}

fn lines(output: &Output) -> impl Iterator<Item = String> + '_ {
    let text = std::str::from_utf8(&output.stdout).unwrap_or_default();
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(String::from)
}

/// Reads one `State|ExitCode|Start|End` line of `sacct -P`.
pub(super) fn parse_accounted(line: &str) -> Option<JobState> {
    let mut fields = line.split('|');
    // A cancelled state reads `CANCELLED by <uid>`.
    let state = fields.next()?.split_whitespace().next()?.to_string();
    // `ExitCode` reads `code:signal`.
    let mut exit = fields.next()?.split(':');
    let exit_code = exit.next().and_then(|code| code.parse().ok());
    let signal = exit.next().and_then(|signal| signal.parse().ok());
    let started_at_ms = fields.next().and_then(parse_time);
    let finished_at_ms = fields.next().and_then(parse_time);
    Some(JobState {
        state,
        exit_code,
        signal,
        started_at_ms,
        finished_at_ms,
    })
}

/// Slurm prints local times, or `Unknown` and `None` before they exist.
fn parse_time(text: &str) -> Option<u64> {
    let time = NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S").ok()?;
    let millis = Local
        .from_local_datetime(&time)
        .earliest()?
        .timestamp_millis();
    u64::try_from(millis).ok()
}

/// The attempt phase of a Slurm job state; `cancel_requested` tells an asked
/// cancellation from one the scheduler or an operator made.
pub(super) fn job_phase(job: &JobState, cancel_requested: bool) -> AttemptPhase {
    match job.state.as_str() {
        "PENDING" | "CONFIGURING" | "REQUEUED" | "REQUEUE_HOLD" | "REQUEUE_FED"
        | "RESV_DEL_HOLD" | "SUSPENDED" => AttemptPhase::Submitted,
        "RUNNING" | "COMPLETING" | "STAGE_OUT" | "SIGNALING" | "RESIZING" => AttemptPhase::Running,
        "COMPLETED" | "FAILED" => match (job.exit_code, job.signal) {
            (Some(_), Some(signal)) if signal != 0 => AttemptPhase::Failed {
                reason: format!("the task ended on signal {signal}"),
            },
            (Some(0), Some(0)) if job.state == "FAILED" => AttemptPhase::Failed {
                reason: "Slurm reports the job as failed".to_string(),
            },
            (Some(code), Some(_)) => AttemptPhase::Exited { code },
            _ => AttemptPhase::SystemError {
                reason: format!("Slurm reports {} without an exit code", job.state),
            },
        },
        "OUT_OF_MEMORY" => AttemptPhase::Failed {
            reason: "the task exceeded its memory limit".to_string(),
        },
        "TIMEOUT" => AttemptPhase::Failed {
            reason: "the task exceeded its walltime".to_string(),
        },
        "CANCELLED" if cancel_requested => AttemptPhase::Cancelled,
        state => AttemptPhase::SystemError {
            reason: format!("Slurm ended the job as {state}"),
        },
    }
}

pub(super) fn write_private(path: &Path, text: &str) -> Result<(), BackendError> {
    std::fs::write(path, text).map_err(io_error)?;
    let mode = std::fs::Permissions::from_mode(0o600);
    std::fs::set_permissions(path, mode).map_err(io_error)
}

/// Quotes `text` as one POSIX shell word.
pub(super) fn quote(text: &str) -> String {
    format!("'{}'", text.replace('\'', r"'\''"))
}

/// Exports the task environment, secrets included, for Apptainer to pass into the container.
pub(super) fn env_script(spec: &TaskSpec) -> Result<String, BackendError> {
    let mut script = String::new();
    for (key, value) in spec.effective_env() {
        validate_env_key(&key)?;
        script.push_str(&format!("export APPTAINERENV_{key}={}\n", quote(&value)));
    }
    Ok(script)
}

/// Runs the image's own entrypoint with `apptainer run` unless the task names one, which
/// `apptainer exec` starts instead.
pub(super) fn job_script(
    apptainer: &str,
    spec: &TaskSpec,
    binds: &[BindMount],
    directory: &Path,
) -> String {
    let mut argv = vec![apptainer.to_string()];
    argv.push(
        if spec.entrypoint.is_some() {
            "exec"
        } else {
            "run"
        }
        .to_string(),
    );
    argv.extend(
        [
            "--containall",
            "--cleanenv",
            "--no-home",
            "--userns",
            "--no-privs",
        ]
        .map(String::from),
    );
    if spec.security.network == NetworkAccess::Isolated {
        argv.extend(["--net", "--network", "none"].map(String::from));
    }
    for bind in binds {
        let mode = if bind.writable { "rw" } else { "ro" };
        let (host, container) = (bind.host.display(), bind.container.display());
        argv.extend(["--bind".to_string(), format!("{host}:{container}:{mode}")]);
    }
    if let Some(workdir) = &spec.workdir {
        argv.extend(["--pwd".to_string(), workdir.clone()]);
    }
    argv.push(format!("docker://{}", spec.image));
    argv.extend(spec.entrypoint.iter().flatten().cloned());
    argv.extend(spec.command.iter().cloned());
    let command: Vec<String> = argv.iter().map(|word| quote(word)).collect();
    let env = quote(&directory.join("env.sh").display().to_string());
    format!("#!/bin/sh\nset -e\n. {env}\nexec {}\n", command.join(" "))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_accounting_line() {
        let job = parse_accounted("CANCELLED by 1000|0:15|2026-11-03T10:00:00|Unknown").unwrap();
        assert_eq!(job.state, "CANCELLED");
        assert_eq!(job.exit_code, Some(0));
        assert_eq!(job.signal, Some(15));
        assert!(job.started_at_ms.is_some());
        assert_eq!(job.finished_at_ms, None);
        assert_eq!(parse_accounted(""), None);
    }

    #[test]
    fn maps_job_states() {
        let job = |state: &str, exit_code| JobState {
            state: state.to_string(),
            exit_code,
            signal: exit_code.map(|_| 0),
            started_at_ms: None,
            finished_at_ms: None,
        };
        assert_eq!(
            job_phase(&job("PENDING", None), false),
            AttemptPhase::Submitted
        );
        assert_eq!(
            job_phase(&job("FAILED", Some(3)), false),
            AttemptPhase::Exited { code: 3 }
        );
        assert_eq!(
            job_phase(&job("COMPLETED", Some(0)), false),
            AttemptPhase::Exited { code: 0 }
        );
        // A signal or a failed state never reads as success.
        let killed = JobState {
            signal: Some(9),
            ..job("FAILED", Some(0))
        };
        assert!(matches!(
            job_phase(&killed, false),
            AttemptPhase::Failed { .. }
        ));
        assert!(matches!(
            job_phase(&job("FAILED", Some(0)), false),
            AttemptPhase::Failed { .. }
        ));
        assert!(matches!(
            job_phase(&job("TIMEOUT", Some(0)), false),
            AttemptPhase::Failed { .. }
        ));
        assert_eq!(
            job_phase(&job("CANCELLED", Some(0)), true),
            AttemptPhase::Cancelled
        );
        // Nobody asked: the infrastructure ended it, so the work may still succeed elsewhere.
        assert!(matches!(
            job_phase(&job("CANCELLED", Some(0)), false),
            AttemptPhase::SystemError { .. }
        ));
        assert!(matches!(
            job_phase(&job("NODE_FAIL", None), false),
            AttemptPhase::SystemError { .. }
        ));
    }
}
