//! Tests the Slurm backend against fake `sbatch`, `squeue`, `sacct` and `scancel` scripts.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::scheduler::quote;
use super::*;
use aruna_core::compute::{AttemptRef, TaskInput};
use bytes::Bytes;
use tempfile::TempDir;

const IMAGE: &str = "repo@sha256:0000000000000000000000000000000000000000000000000000000000000000";

/// Jobs are lines of `id|name|state|exit|start|end` in one file the fakes share.
const SBATCH: &str = r#"[ "$1" = "--version" ] && { echo "slurm 24.05.0"; exit 0; }
for arg in "$@"; do case "$arg" in --job-name=*) name="${arg#--job-name=}";; esac; done
echo "$@" >> "$state/sbatch.args"
id=$(( $(wc -l < "$state/jobs") + 100 ))
echo "$id|$name|PENDING|0:0|Unknown|Unknown" >> "$state/jobs"
echo "$id""#;

const SQUEUE: &str = r#"while [ $# -gt 0 ]; do case "$1" in
  -o) format="$2"; shift;; -j) id="$2"; shift;; --name=*) name="${1#--name=}";; esac; shift; done
awk -F'|' -v id="$id" -v name="$name" -v format="$format" '
  ($3 == "PENDING" || $3 == "RUNNING") && (id == "" || $1 == id) && (name == "" || $2 == name) {
    print (format == "%i" ? $1 : $3) }' "$state/jobs""#;

const SACCT: &str = r#"while [ $# -gt 0 ]; do case "$1" in
  -o) format="$2"; shift;; -j) id="$2"; shift;; -S) shift;; --name=*) name="${1#--name=}";; esac; shift; done
awk -F'|' -v id="$id" -v name="$name" -v format="$format" '
  (id == "" || $1 == id) && (name == "" || $2 == name) {
    if (format == "JobID") print $1; else print $3 "|" $4 "|" $5 "|" $6 }' "$state/jobs""#;

const SCANCEL: &str = r#"awk -F'|' -v OFS='|' -v id="$1" '$1 == id { $3 = "CANCELLED by 1000" } { print }' \
  "$state/jobs" > "$state/jobs.tmp" && mv "$state/jobs.tmp" "$state/jobs""#;

struct Fixture {
    dir: TempDir,
    backend: SlurmBackend,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("bin");
        std::fs::create_dir_all(&bin).unwrap();
        std::fs::write(dir.path().join("jobs"), "").unwrap();
        let fake = |name: &str, body: &str| {
            let path = bin.join(name);
            let state = quote(&dir.path().display().to_string());
            std::fs::write(&path, format!("#!/bin/sh\nstate={state}\n{body}\n")).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
            path
        };
        let config = SlurmConfig {
            state_root: dir.path().join("state"),
            sbatch: fake("sbatch", SBATCH),
            squeue: fake("squeue", SQUEUE),
            sacct: fake("sacct", SACCT),
            scancel: fake("scancel", SCANCEL),
            partition: Some("batch".to_string()),
            ..SlurmConfig::default()
        };
        let backend = SlurmBackend::with_config(config).unwrap();
        Self { dir, backend }
    }

    fn jobs(&self) -> Vec<String> {
        let jobs = std::fs::read_to_string(self.dir.path().join("jobs")).unwrap();
        jobs.lines().map(String::from).collect()
    }

    /// Replaces the state, exit code and times of job `id`.
    fn set_job(&self, id: &str, fields: &str) {
        let jobs: Vec<String> = (self.jobs().into_iter())
            .map(|line| {
                let mut parts = line.splitn(3, '|');
                let (job, name) = (parts.next().unwrap(), parts.next().unwrap());
                match job == id {
                    true => format!("{job}|{name}|{fields}"),
                    false => line.clone(),
                }
            })
            .collect();
        std::fs::write(self.dir.path().join("jobs"), jobs.join("\n") + "\n").unwrap();
    }
}

fn context() -> FenceContext {
    FenceContext {
        attempt: AttemptRef::new("job", 1),
        attempt_epoch: 2,
        controller_generation: 1,
    }
}

fn spec(context: &FenceContext) -> TaskSpec {
    let mut spec = TaskSpec::new(context.attempt.clone(), IMAGE);
    spec.command = vec![
        "sh".to_string(),
        "-c".to_string(),
        "echo 'it''s'".to_string(),
    ];
    spec.security.run_as = process_identity();
    spec.env = BTreeMap::from([("GREETING".to_string(), "it's".to_string())]);
    spec.output_paths = vec!["/out/result.txt".to_string()];
    let data = futures_util::stream::iter([Ok::<_, std::io::Error>(Bytes::from_static(b"data"))]);
    spec.inputs = vec![TaskInput::from_stream("/in/data.txt", 4, Box::pin(data))];
    spec
}

#[tokio::test]
async fn runs_to_exit() {
    let fixture = Fixture::new();
    let context = context();
    let cancel = CancellationToken::new();
    let backend = &fixture.backend;

    let submitted = backend.submit(&context, &spec(&context), &cancel).await;
    let submitted = submitted.unwrap();
    assert_eq!(submitted.phase, AttemptPhase::Submitted);
    assert_eq!(submitted.backend_ref, "slurm:100");
    let args = std::fs::read_to_string(fixture.dir.path().join("sbatch.args")).unwrap();
    assert!(args.contains("--job-name=aruna-job-a1-e2"), "{args}");
    assert!(args.contains("--mem=2048M --cpus-per-task=2 --partition=batch"));

    // The job sees the staged input and the quoted command at the final attempt path.
    let directory = backend.state.attempt_dir(&context);
    let script = std::fs::read_to_string(directory.join("job.sh")).unwrap();
    let input = directory.join("workspace/root/in/data.txt");
    assert!(script.contains(&format!("'{}:/in/data.txt:ro'", input.display())));
    assert!(script.contains(r"'echo '\''it'\'''\''s'\'''"), "{script}");
    assert_eq!(std::fs::read(input).unwrap(), b"data");
    let env = std::fs::read_to_string(directory.join("env.sh")).unwrap();
    assert!(
        env.contains(r"export APPTAINERENV_GREETING='it'\''s'"),
        "{env}"
    );

    fixture.set_job("100", "RUNNING|0:0|2026-11-03T10:00:00|Unknown");
    let running = backend.status(&context).await.unwrap();
    assert_eq!(running.phase, AttemptPhase::Running);
    fixture.set_job("100", "FAILED|3:0|2026-11-03T10:00:00|2026-11-03T10:01:00");
    let exited = backend.status(&context).await.unwrap();
    assert_eq!(exited.phase, AttemptPhase::Exited { code: 3 });
    assert!(exited.finished_at_ms.is_some());

    std::fs::write(directory.join("logs/stdout"), "hello").unwrap();
    let logs = backend.fetch_logs(&context, &LogLimits::default()).await;
    assert_eq!(logs.unwrap().stdout, b"hello");
    backend.cleanup(&context).await.unwrap();
    assert!(!directory.join("workspace").exists());
}

#[tokio::test]
async fn interrupted_submit_adopts() {
    // A controller lost after `sbatch` finds the job by name and never submits twice.
    let fixture = Fixture::new();
    let context = context();
    let cancel = CancellationToken::new();
    let backend = &fixture.backend;
    backend
        .submit(&context, &spec(&context), &cancel)
        .await
        .unwrap();
    let directory = backend.state.attempt_dir(&context);
    std::fs::remove_file(directory.join("slurm.json")).unwrap();

    let ReconcileEvidence::Adoptable(evidence) = backend.reconcile(&context).await else {
        panic!("the queued job is adoptable");
    };
    assert_eq!(evidence.status.backend_ref, "slurm:100");
    assert_eq!(evidence.status.phase, AttemptPhase::Submitted);
    let resubmitted = backend.submit(&context, &spec(&context), &cancel).await;
    assert_eq!(resubmitted.unwrap().backend_ref, "slurm:100");
    assert_eq!(fixture.jobs().len(), 1);

    // A second job under the same name is never adopted.
    let duplicate = "101|aruna-job-a1-e2|PENDING|0:0|Unknown|Unknown";
    let jobs = fixture.jobs().join("\n") + "\n" + duplicate + "\n";
    std::fs::write(fixture.dir.path().join("jobs"), jobs).unwrap();
    let ReconcileEvidence::Unadoptable(evidence) = backend.reconcile(&context).await else {
        panic!("two jobs of one attempt are not adoptable");
    };
    assert!(evidence.multiple);
}

#[tokio::test]
async fn cancel_marks_request() {
    let fixture = Fixture::new();
    let context = context();
    let cancel = CancellationToken::new();
    let backend = &fixture.backend;
    assert_eq!(
        backend.cancel(&context).await.unwrap(),
        CancelEvidence::AlreadyGone
    );
    backend
        .submit(&context, &spec(&context), &cancel)
        .await
        .unwrap();

    assert_eq!(
        backend.cancel(&context).await.unwrap(),
        CancelEvidence::Requested
    );
    let status = backend.status(&context).await.unwrap();
    assert_eq!(status.phase, AttemptPhase::Cancelled);
    let stopped = backend.cancel(&context).await.unwrap();
    assert!(matches!(stopped, CancelEvidence::Stopped(_)));
}

#[tokio::test]
async fn foreign_cancel_errors() {
    // A job the scheduler or an operator cancelled ended without job-specific evidence.
    let fixture = Fixture::new();
    let context = context();
    let cancel = CancellationToken::new();
    let backend = &fixture.backend;
    backend
        .submit(&context, &spec(&context), &cancel)
        .await
        .unwrap();
    fixture.set_job("100", "CANCELLED by 0|0:15|Unknown|2026-11-03T10:01:00");

    let status = backend.status(&context).await.unwrap();
    assert!(matches!(status.phase, AttemptPhase::SystemError { .. }));
}

#[tokio::test]
async fn older_generation_fenced() {
    let fixture = Fixture::new();
    let context = context();
    let newer = FenceContext {
        controller_generation: 2,
        ..context.clone()
    };
    fixture.backend.fence(&newer).await.unwrap();

    let fenced = fixture.backend.fence(&context).await;
    assert!(matches!(fenced, Err(BackendError::Fenced)));
    let submitted = (fixture.backend)
        .submit(&context, &spec(&context), &CancellationToken::new())
        .await;
    assert!(matches!(submitted, Err(BackendError::Fenced)));
    assert!(fixture.jobs().is_empty());
}

#[test]
fn relative_state_root() {
    let dir = tempfile::tempdir().unwrap();
    let parents = std::env::current_dir().unwrap().components().count() - 1;
    let target = dir.path().canonicalize().unwrap();
    let relative = PathBuf::from("../".repeat(parents))
        .join(target.strip_prefix("/").unwrap())
        .join("state");
    let config = SlurmConfig {
        state_root: relative,
        ..SlurmConfig::default()
    };
    let backend = SlurmBackend::with_config(config).unwrap();
    assert_eq!(backend.config.state_root, target.join("state"));
    assert!(backend.state.attempt_dir(&context()).starts_with(target));
}
