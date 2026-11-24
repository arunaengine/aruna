//! The task backend of a coordinator run: every attempt becomes a `StartTask` call that the node
//! runs as a child job, and its answer becomes the scratch folder that uwe reads results from.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::path::{Path, PathBuf};

use aruna_workflow_contract::host::{
    DirectInput, ExpectedOutput, SecretPlaceholder, StagedFile, StartTask, TaskFinished,
    TaskFolders, TaskOutput, TaskResources,
};
use aruna_workflow_contract::settings::ArunaStepSettings;
use aruna_workflow_contract::{ContractError, HostRpcRequest, HostRpcResult};
use executor::manifest::Collected;
use executor::wrapper::{self, Folders};
use executor::{
    BoxFuture, CONTROL, CancellationToken, DynBackend, ExecutorError, Finished, Job, OrphansResult,
    Staged, Usage, WORK,
};
use task_model::EnvironmentValue;

use crate::composed::{StepMatch, settings_for};
use crate::protocol::RpcClient;
use crate::storage::{self, served};

/// Where the wrapper finds its folders inside the task container.
pub const WORK_FOLDER: &str = "/uwe/work";
pub const CONTROL_FOLDER: &str = "/uwe/control";
pub const HOME_FOLDER: &str = "/uwe/home";
/// The scheme of the addresses that inputs only the coordinator held are uploaded to.
pub const STAGING_SCHEME: &str = "aruna-staging://";

pub struct CoordinatorBackend {
    rpc: RpcClient,
    run: String,
    /// Where each attempt gets the scratch folder that uwe reads its results from.
    scratch: PathBuf,
    steps: Vec<StepMatch>,
    secrets: Vec<SecretPlaceholder>,
}

/// The parts of an attempt name, `<run>-<step>-<task>-<attempt>`.
#[derive(Debug, PartialEq, Eq)]
struct AttemptName {
    step: String,
    task: Option<u64>,
    attempt: u32,
}

impl CoordinatorBackend {
    pub fn new(
        rpc: RpcClient,
        run: String,
        scratch: PathBuf,
        steps: Vec<StepMatch>,
        secrets: Vec<SecretPlaceholder>,
    ) -> CoordinatorBackend {
        CoordinatorBackend {
            rpc,
            run,
            scratch,
            steps,
            secrets,
        }
    }

    async fn run_attempt(
        &self,
        job: Job<'_>,
        cancel: &CancellationToken,
    ) -> Result<Finished, ExecutorError> {
        if cancel.is_cancelled() {
            return Err(ExecutorError::Cancelled);
        }
        if job.spec.containers.len() > 1 {
            return Err(ExecutorError::Unsupported(
                "Aruna runs one container per task; this task has several".into(),
            ));
        }
        let name = parse_name(&self.run, job.name);
        let request = self.start_request(job, &name).await?;
        let started = self.rpc.call(HostRpcRequest::StartTask(Box::new(request)));
        let answer = tokio::select! {
            answer = started => answer,
            () = cancel.cancelled() => {
                let stop = HostRpcRequest::CancelTask { name: job.name.to_owned() };
                if let Err(error) = self.rpc.call(stop).await {
                    tracing::warn!(%error, task = job.name, "the host did not cancel the task");
                }
                return Err(ExecutorError::Cancelled);
            }
        };
        let finished = match answer {
            Ok(HostRpcResult::TaskFinished(finished)) => *finished,
            Ok(other) => return Err(engine_error(format!("unexpected answer {other:?}"))),
            Err(ContractError::Cancelled { .. }) => return Err(ExecutorError::Cancelled),
            Err(error) => return Err(engine_error(error.to_string())),
        };
        if finished.cancelled {
            return Err(ExecutorError::Cancelled);
        }
        if let Some(error) = &finished.error {
            return Err(match finished.retryable {
                true => engine_error(error.clone()),
                false => ExecutorError::Unsupported(error.clone()),
            });
        }
        self.collect(job.name, finished).await
    }

    /// The call that runs an attempt: wrapper, inputs by address, uploads of local inputs,
    /// output patterns, size and the step's Aruna settings.
    async fn start_request(
        &self,
        job: Job<'_>,
        name: &AttemptName,
    ) -> Result<StartTask, ExecutorError> {
        let spec = job.spec;
        let canonical = spec
            .canonical()
            .map_err(|error| ExecutorError::Unsupported(error.to_string()))?;
        let task_key_seed = String::from_utf8(canonical)
            .map_err(|_| ExecutorError::Unsupported("the task specification is no text".into()))?;
        let spec_json = serde_json::to_value(spec)
            .map_err(|error| ExecutorError::Unsupported(error.to_string()))?;
        let folders = Folders {
            work: WORK_FOLDER,
            control: CONTROL_FOLDER,
            home: HOME_FOLDER,
            outbox: "",
            owner: "",
        };
        let script = wrapper::wrapper(spec, &folders);
        let settings = settings_for(&self.steps, &name.step).map_err(ExecutorError::Unsupported)?;
        let placement = job.placement;
        let mut resources = TaskResources {
            cpus: placement.cpus,
            memory_bytes: placement.memory_bytes,
            disk_bytes: placement.disk_bytes,
            time_seconds: placement.time_seconds,
            gpus: (placement.accelerators.iter()).map(|gpu| gpu.count).sum(),
        };
        let mut site = placement.executor.clone();
        let mut network_allowed = !placement.offline;
        if let Some(settings) = &settings {
            apply_settings(settings, &mut resources, &mut site, &mut network_allowed);
        }
        let direct_inputs = (job.staging.direct.iter())
            .map(|direct| DirectInput {
                path: direct.path.clone(),
                address: direct.source.clone(),
                digest: direct.digest.to_string(),
                size: direct.size,
                executable: direct.executable,
                aruna_id: direct.reference.aruna_id.clone(),
                endpoints: direct.endpoints.clone(),
            })
            .collect();
        let staged_files = self.stage_inputs(job).await?;
        Ok(StartTask {
            task_key_seed,
            step_id: name.step.clone(),
            task_id: name.task,
            attempt: name.attempt,
            name: job.name.to_owned(),
            spec_json,
            command_wrapper_bytes: script.into_bytes(),
            folders: TaskFolders {
                work: WORK_FOLDER.into(),
                control: CONTROL_FOLDER.into(),
                home: HOME_FOLDER.into(),
            },
            image: spec.image.as_ref().map(|image| image.reference.clone()),
            resources,
            site,
            network_allowed,
            direct_inputs,
            staged_files,
            expected_outputs: (spec.outputs.iter())
                .map(|rule| ExpectedOutput {
                    pattern: rule.pattern.clone(),
                })
                .collect(),
            secret_placeholders: self.placeholders_in(spec),
            step_settings: settings,
        })
    }

    /// Uploads the inputs that are files of this machine, so that the node can place them.
    async fn stage_inputs(&self, job: Job<'_>) -> Result<Vec<StagedFile>, ExecutorError> {
        let mut staged = Vec::new();
        for directory in &job.staging.directories {
            staged.push(folder_entry(directory.clone()));
        }
        let mut files: Vec<(String, PathBuf, bool)> = Vec::new();
        for input in &job.staging.inputs {
            if input.directory {
                staged.push(folder_entry(input.path.clone()));
                walk_folder(input, &mut files, &mut staged)?;
            } else {
                files.push((input.path.clone(), input.source.clone(), input.executable));
            }
        }
        if let Some(stdin) = &job.staging.stdin {
            let path = format!("{CONTROL_FOLDER}/{}", wrapper::STDIN);
            files.push((path, stdin.clone(), false));
        }
        for (index, (path, source, executable)) in files.into_iter().enumerate() {
            let address = format!("{STAGING_SCHEME}{}/{index}", job.name);
            let receipt = storage::upload_file(&self.rpc, &source, &address)
                .await
                .map_err(|error| engine_error(format!("cannot stage {path}: {error}")))?;
            staged.push(StagedFile {
                path,
                address: Some(receipt.address),
                directory: false,
                executable,
                size: receipt.size,
            });
        }
        Ok(staged)
    }

    /// The placeholders of the run's secrets that the task's environment or command names.
    fn placeholders_in(&self, spec: &task_model::TaskSpec) -> Vec<SecretPlaceholder> {
        let mut texts: Vec<&str> = spec.arguments.iter().map(String::as_str).collect();
        for value in spec.environment.values() {
            if let EnvironmentValue::Literal(text) = value {
                texts.push(text);
            }
        }
        (self.secrets.iter())
            .filter(|secret| texts.iter().any(|text| text.contains(&secret.placeholder)))
            .cloned()
            .collect()
    }

    /// The scratch folder of a finished attempt: the wrapper's control files and the outputs,
    /// read from the node, where uwe expects them.
    async fn collect(&self, name: &str, finished: TaskFinished) -> Result<Finished, ExecutorError> {
        let root = tempfile::Builder::new()
            .prefix(&format!("{}-", segment(name)))
            .tempdir_in(&self.scratch)?
            .keep();
        let control = root.join(CONTROL);
        std::fs::create_dir_all(&control)?;
        std::fs::create_dir_all(root.join(WORK))?;
        std::fs::write(control.join(wrapper::STDOUT), &finished.stdout_tail)?;
        std::fs::write(control.join(wrapper::STDERR), &finished.stderr_tail)?;
        if let Some(code) = finished.exit_code {
            std::fs::write(control.join(wrapper::EXIT), format!("{code}\n"))?;
        }
        if let (Some(started), Some(ended)) = (finished.started, finished.ended) {
            std::fs::write(control.join(wrapper::START), format!("{started}\n"))?;
            std::fs::write(control.join(wrapper::END), format!("{ended}\n"))?;
        }
        let mut files = Vec::new();
        if finished.exit_code == Some(0) {
            for output in &finished.outputs {
                self.fetch_output(&root, output).await?;
                files.push(Collected {
                    path: output.path.clone(),
                    directory: output.directory,
                });
            }
        }
        let usage = finished.usage.map(|usage| Usage {
            cpu_usec: usage.cpu_usec,
            memory_peak: usage.memory_peak,
            read_bytes: usage.read_bytes,
            write_bytes: usage.write_bytes,
            ..Usage::default()
        });
        Ok(Finished {
            exit_code: finished.exit_code,
            files,
            root,
            usage,
            address: None,
            receipts: Vec::new(),
        })
    }

    async fn fetch_output(&self, root: &Path, output: &TaskOutput) -> Result<(), ExecutorError> {
        let invalid =
            |why: &str| ExecutorError::Unsupported(format!("output {}: {why}", output.path));
        if output.path.is_empty() || output.path.split('/').any(|part| part == "..") {
            return Err(invalid("the path leaves the task folder"));
        }
        let local = match output.path.strip_prefix('/') {
            Some(absolute) => root.join(CONTROL).join(wrapper::ROOT).join(absolute),
            None => root.join(WORK).join(&output.path),
        };
        if output.directory {
            return self.fetch_folder(&output.address, &local).await;
        }
        if let Some(parent) = local.parent() {
            std::fs::create_dir_all(parent)?;
        }
        storage::download_file(&self.rpc, &output.address, &local)
            .await
            .map_err(|error| {
                engine_error(format!("cannot read output {}: {error}", output.path))
            })?;
        Ok(())
    }

    /// Reads a folder of the node file by file, through its listings.
    async fn fetch_folder(&self, address: &str, local: &Path) -> Result<(), ExecutorError> {
        let mut pending = vec![(address.trim_end_matches('/').to_owned(), local.to_owned())];
        while let Some((folder, target)) = pending.pop() {
            std::fs::create_dir_all(&target)?;
            let request = HostRpcRequest::StorageList {
                address: format!("{folder}/"),
            };
            let entries = match self.rpc.call(request).await {
                Ok(HostRpcResult::StorageList { entries }) => entries,
                Ok(other) => return Err(engine_error(format!("unexpected answer {other:?}"))),
                Err(error) => return Err(engine_error(error.to_string())),
            };
            for entry in entries {
                let name = entry.name.trim_end_matches('/');
                if name.is_empty() || name == "." || name == ".." || name.contains(['/', '\\']) {
                    return Err(engine_error(format!("the listing names {name:?}")));
                }
                let (child, path) = (format!("{folder}/{name}"), target.join(name));
                if entry.directory {
                    pending.push((child, path));
                } else {
                    storage::download_file(&self.rpc, &child, &path)
                        .await
                        .map_err(|error| engine_error(error.to_string()))?;
                }
            }
        }
        Ok(())
    }
}

impl DynBackend for CoordinatorBackend {
    fn pin<'a>(&'a self, image: &'a str) -> BoxFuture<'a, Result<Option<String>, ExecutorError>> {
        self.pin_at(image, None)
    }

    fn pin_at<'a>(
        &'a self,
        image: &'a str,
        site: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Option<String>, ExecutorError>> {
        Box::pin(async move {
            let request = HostRpcRequest::PinImage {
                image: image.to_owned(),
                site: site.map(str::to_owned),
            };
            match self.rpc.call(request).await {
                Ok(HostRpcResult::PinImage { pinned }) => Ok(pinned),
                Ok(other) => Err(engine_error(format!("unexpected answer {other:?}"))),
                Err(ContractError::NotFound { message }) => Err(ExecutorError::MissingImage {
                    image: image.to_owned(),
                    reason: message,
                }),
                Err(error) => Err(engine_error(error.to_string())),
            }
        })
    }

    fn run<'a>(
        &'a self,
        job: Job<'a>,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Finished, ExecutorError>> {
        Box::pin(self.run_attempt(job, cancel))
    }

    fn hold(&self) {}

    fn release(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }

    fn direct_sources(&self) -> bool {
        true
    }

    /// The node reads `aruna://` and `s3://` inputs itself; other inputs pass this machine.
    fn supports_direct(&self, reference: &::storage::DirectRef) -> bool {
        served(&reference.url) && reference.headers.is_empty()
    }

    fn orphans<'a>(&'a self, _run: &'a str) -> BoxFuture<'a, OrphansResult> {
        Box::pin(async { Ok(Vec::new()) })
    }

    /// Asks the node to stop the child jobs of the run; it reports them in its own records.
    fn stop_orphans<'a>(&'a self, run: &'a str) -> BoxFuture<'a, OrphansResult> {
        Box::pin(async move {
            let request = HostRpcRequest::StopOrphans {
                run_id: run.to_owned(),
            };
            match self.rpc.call(request).await {
                Ok(HostRpcResult::StopOrphans { .. }) => Ok(Vec::new()),
                Ok(other) => Err(engine_error(format!("unexpected answer {other:?}"))),
                Err(error) => Err(engine_error(error.to_string())),
            }
        })
    }
}

/// Lets the step's settings decide size, site and network where they say anything.
fn apply_settings(
    settings: &ArunaStepSettings,
    resources: &mut TaskResources,
    site: &mut Option<String>,
    network: &mut bool,
) {
    let given = &settings.resources;
    if let Some(cpus) = given.cpus {
        resources.cpus = (cpus.ceil() as u32).max(1);
    }
    resources.memory_bytes = settings.memory_bytes().or(resources.memory_bytes);
    resources.disk_bytes = settings.disk_bytes().or(resources.disk_bytes);
    resources.time_seconds = settings.walltime_seconds().or(resources.time_seconds);
    resources.gpus = given.gpus.unwrap_or(resources.gpus);
    if let Some(named) = &settings.placement.site {
        *site = Some(named.clone());
    }
    if settings.execution.network == Some(false) {
        *network = false;
    }
}

fn folder_entry(path: String) -> StagedFile {
    StagedFile {
        path,
        address: None,
        directory: true,
        executable: false,
        size: 0,
    }
}

/// The files and folders below a local folder input, each at its path in the task.
fn walk_folder(
    input: &Staged,
    files: &mut Vec<(String, PathBuf, bool)>,
    staged: &mut Vec<StagedFile>,
) -> Result<(), ExecutorError> {
    let mut pending = vec![(input.source.clone(), input.path.clone())];
    while let Some((folder, path)) = pending.pop() {
        for entry in std::fs::read_dir(&folder)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let child = format!("{path}/{name}");
            let kind = entry.file_type()?;
            if kind.is_dir() {
                staged.push(folder_entry(child.clone()));
                pending.push((entry.path(), child));
            } else if kind.is_file() {
                files.push((child, entry.path(), input.executable));
            } else {
                return Err(ExecutorError::Unsupported(format!(
                    "the input {child} is neither a file nor a folder"
                )));
            }
        }
    }
    Ok(())
}

/// Splits `<run>-<identity>-<attempt>`; the identity is `<step>-<task>` where uwe numbers it.
fn parse_name(run: &str, name: &str) -> AttemptName {
    let rest = name
        .strip_prefix(run)
        .and_then(|rest| rest.strip_prefix('-'))
        .unwrap_or(name);
    let (identity, attempt) = match rest.rsplit_once('-') {
        Some((identity, attempt)) => match attempt.parse() {
            Ok(attempt) => (identity, attempt),
            Err(_) => (rest, 1),
        },
        None => (rest, 1),
    };
    let (step, task) = match identity.rsplit_once('-') {
        Some((step, task)) => match task.parse() {
            Ok(task) => (step, Some(task)),
            Err(_) => (identity, None),
        },
        None => (identity, None),
    };
    AttemptName {
        step: step.to_owned(),
        task,
        attempt,
    }
}

/// A name as one path segment, as uwe names attempts after task identities.
fn segment(text: &str) -> String {
    text.chars()
        .map(
            |letter| match letter.is_ascii_alphanumeric() || "._-".contains(letter) {
                true => letter,
                false => '_',
            },
        )
        .collect()
}

fn engine_error(message: String) -> ExecutorError {
    ExecutorError::Engine {
        engine: "aruna".into(),
        message,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_attempt_names() {
        let parsed = parse_name("run-1", "run-1-sayHello-12-2");
        let expected = AttemptName {
            step: "sayHello".into(),
            task: Some(12),
            attempt: 2,
        };
        assert_eq!(parsed, expected);
        let unnumbered = parse_name("run-1", "run-1-greet-1");
        assert_eq!((unnumbered.step.as_str(), unnumbered.task), ("greet", None));
        assert_eq!(parse_name("other", "x").step, "x");
    }

    #[test]
    fn settings_override_size() {
        let settings = ArunaStepSettings::from_value(serde_json::json!({
            "resources": {"cpus": 1.5, "memory_gib": 2, "gpus": 1, "walltime_minutes": 3},
            "placement": {"site": "giessen"},
            "execution": {"network": false},
        }))
        .expect("reads");
        let mut resources = TaskResources {
            cpus: 1,
            disk_bytes: Some(7),
            ..TaskResources::default()
        };
        let (mut site, mut network) = (None, true);
        apply_settings(&settings, &mut resources, &mut site, &mut network);
        assert_eq!(resources.cpus, 2);
        assert_eq!(resources.memory_bytes, Some(2 << 30));
        assert_eq!(resources.disk_bytes, Some(7));
        assert_eq!((resources.gpus, resources.time_seconds), (1, Some(180)));
        assert_eq!((site.as_deref(), network), (Some("giessen"), false));
    }
}
