//! The task backend of a coordinator run: every attempt becomes a `StartTask` call that the node
//! runs as a child job, and its answer becomes the scratch folder that uwe reads results from.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::path::{Component, Path, PathBuf};

use aruna_workflow_contract::host::{
    DirectInput, DirectoryFile, ExpectedOutput, SecretPlaceholder, StagedFile, StartTask,
    TaskFinished, TaskFolders, TaskOutput, TaskResources,
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
        let (clean, secret_placeholders) = extract_secrets(&self.secrets, spec)?;
        let spec_json = serde_json::to_value(&clean)
            .map_err(|error| ExecutorError::Unsupported(error.to_string()))?;
        let folders = Folders {
            work: WORK_FOLDER,
            control: CONTROL_FOLDER,
            home: HOME_FOLDER,
            outbox: "",
            owner: "",
        };
        let script = wrapper::wrapper(&clean, &folders);
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
            secret_placeholders,
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
        let Some(local) = output_local_path(root, &output.path) else {
            return Err(invalid("the path leaves the task folder"));
        };
        if output.directory {
            if let Some(token) = output.files_token.as_deref() {
                std::fs::create_dir_all(&local)?;
                let mut page = 0u32;
                loop {
                    let request = HostRpcRequest::TaskDirectoryPage {
                        token: token.to_owned(),
                        page,
                    };
                    let (files, next_page) = match self.rpc.call(request).await {
                        Ok(HostRpcResult::TaskDirectoryPage { files, next_page }) => {
                            (files, next_page)
                        }
                        Ok(other) => {
                            return Err(engine_error(format!("unexpected answer {other:?}")));
                        }
                        Err(error) => return Err(engine_error(error.to_string())),
                    };
                    self.download_entries(&local, &output.path, &files).await?;
                    match next_page {
                        Some(next) if next > page => page = next,
                        Some(_) => {
                            return Err(engine_error(
                                "remote directory manifest page did not advance".into(),
                            ));
                        }
                        None => break,
                    }
                }
                return Ok(());
            }
            if !output.files.is_empty() || output.address.is_empty() {
                std::fs::create_dir_all(&local)?;
                self.download_entries(&local, &output.path, &output.files)
                    .await?;
                return Ok(());
            }
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

    async fn download_entries(
        &self,
        local: &Path,
        output_path: &str,
        files: &[DirectoryFile],
    ) -> Result<(), ExecutorError> {
        for entry in files {
            if entry.path.is_empty()
                || entry.path.contains('\0')
                || entry.path.starts_with('/')
                || !entry.path.split('/').all(single_segment)
            {
                return Err(ExecutorError::Unsupported(format!(
                    "output {output_path}: a directory file leaves the task folder"
                )));
            }
            let child = local.join(&entry.path);
            if let Some(parent) = child.parent() {
                std::fs::create_dir_all(parent)?;
            }
            storage::download_file(&self.rpc, &entry.address, &child)
                .await
                .map_err(|error| {
                    engine_error(format!("cannot read output {}: {error}", entry.path))
                })?;
        }
        Ok(())
    }

    /// Reads a folder of the node file by file, through its listings.
    async fn fetch_folder(&self, address: &str, local: &Path) -> Result<(), ExecutorError> {
        let mut pending = vec![(address.to_owned(), local.to_owned())];
        while let Some((folder, target)) = pending.pop() {
            std::fs::create_dir_all(&target)?;
            let request = HostRpcRequest::StorageList {
                address: folder_list_address(&folder),
            };
            let entries = match self.rpc.call(request).await {
                Ok(HostRpcResult::StorageList { entries }) => entries,
                Ok(other) => return Err(engine_error(format!("unexpected answer {other:?}"))),
                Err(error) => return Err(engine_error(error.to_string())),
            };
            for entry in entries {
                let name = entry.name.trim_end_matches('/');
                if name.contains('\0') || name.contains('/') || !single_segment(name) {
                    return Err(engine_error(format!("the listing names {name:?}")));
                }
                let (child, path) = (child_address(&folder, name), target.join(name));
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

/// Moves secret placeholders out of the task environment so the wrapper never overwrites
/// container `vault_env`, and refuses placeholders pasted into commands or partial strings.
fn extract_secrets(
    secrets: &[SecretPlaceholder],
    spec: &task_model::TaskSpec,
) -> Result<(task_model::TaskSpec, Vec<SecretPlaceholder>), ExecutorError> {
    let mut clean = spec.clone();
    if secrets.is_empty() {
        return Ok((clean, Vec::new()));
    }
    for argument in &spec.arguments {
        if let Some(secret) = (secrets.iter()).find(|secret| argument.contains(&secret.placeholder))
        {
            return Err(ExecutorError::Unsupported(format!(
                "secret {} must be passed through the task environment, not command arguments",
                secret.name
            )));
        }
    }
    let mut found = Vec::new();
    for (env_name, value) in &spec.environment {
        let EnvironmentValue::Literal(text) = value else {
            continue;
        };
        let Some(secret) = (secrets.iter()).find(|secret| text.contains(&secret.placeholder))
        else {
            continue;
        };
        if text != &secret.placeholder {
            return Err(ExecutorError::Unsupported(format!(
                "secret {} must be the full value of an environment variable",
                secret.name
            )));
        }
        clean.environment.remove(env_name);
        found.push(SecretPlaceholder {
            name: secret.name.clone(),
            placeholder: secret.placeholder.clone(),
            env_name: (env_name != &secret.name).then(|| env_name.clone()),
        });
    }
    Ok((clean, found))
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
    if let Some(allowed) = settings.execution.network {
        *network = allowed;
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

/// Places the trailing folder slash before any `?query` suffix when listing a folder.
fn folder_list_address(folder: &str) -> String {
    match folder.split_once('?') {
        Some((base, query)) => format!("{}/?{query}", base.trim_end_matches('/')),
        None => format!("{}/", folder.trim_end_matches('/')),
    }
}

/// Joins one listed child name onto a folder address, percent-encoding special key characters
/// while leaving any query suffix intact.
fn child_address(folder: &str, name: &str) -> String {
    let encoded = aruna_workflow_contract::host::encode_key(name);
    match folder.split_once('?') {
        Some((base, query)) => format!("{}/{encoded}?{query}", base.trim_end_matches('/')),
        None => format!("{}/{encoded}", folder.trim_end_matches('/')),
    }
}

fn engine_error(message: String) -> ExecutorError {
    ExecutorError::Engine {
        engine: "aruna".into(),
        message,
    }
}

/// Whether `part` is a single normal path component on this OS (allows POSIX literal `\`).
fn single_segment(part: &str) -> bool {
    let mut comps = Path::new(part).components();
    matches!(comps.next(), Some(Component::Normal(_))) && comps.next().is_none()
}

/// Resolves a manifest output path strictly inside `root`, normalizing repeated leading slashes.
fn output_local_path(root: &Path, path: &str) -> Option<PathBuf> {
    if path.is_empty() || path.contains('\0') {
        return None;
    }
    if path.starts_with('/') {
        let mut parts = Vec::new();
        for part in path.trim_start_matches('/').split('/') {
            match part {
                "" | "." => continue,
                normal if single_segment(normal) => parts.push(normal),
                _ => return None,
            }
        }
        if parts.is_empty() {
            return None;
        }
        let mut local = root.join(CONTROL).join(wrapper::ROOT);
        for part in parts {
            local.push(part);
        }
        return Some(local);
    }
    let mut local = root.join(WORK);
    for part in path.split('/') {
        match part {
            "" | "." => continue,
            normal if single_segment(normal) => local.push(normal),
            _ => return None,
        }
    }
    Some(local)
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
        let open = ArunaStepSettings::from_value(serde_json::json!({
            "execution": {"network": true},
        }))
        .expect("reads");
        let mut isolated = false;
        apply_settings(&open, &mut resources, &mut site, &mut isolated);
        assert!(isolated);
    }

    #[test]
    fn extracts_secret_env() {
        let secret = SecretPlaceholder {
            name: "TOKEN".into(),
            placeholder: "__ARUNA_SECRET_abc_TOKEN__".into(),
            env_name: None,
        };
        let secrets = [secret.clone()];
        let mut spec = task_model::TaskSpec {
            format: 1,
            namespace: String::new(),
            image: None,
            arguments: vec!["echo".into(), "ok".into()],
            workdir: None,
            environment: Default::default(),
            inputs: Vec::new(),
            outputs: Vec::new(),
            output_manifest: None,
            stdin: None,
            containers: Vec::new(),
            replaces_entrypoint: false,
            run_as: None,
        };
        spec.environment.insert(
            "TOKEN".into(),
            EnvironmentValue::Literal(secret.placeholder.clone()),
        );
        spec.environment.insert(
            "ALT".into(),
            EnvironmentValue::Literal(secret.placeholder.clone()),
        );
        spec.environment
            .insert("PLAIN".into(), EnvironmentValue::Literal("hello".into()));
        let (clean, extracted) = extract_secrets(&secrets, &spec).expect("extracts");
        assert!(!clean.environment.contains_key("TOKEN"));
        assert!(!clean.environment.contains_key("ALT"));
        assert_eq!(
            clean.environment.get("PLAIN"),
            Some(&EnvironmentValue::Literal("hello".into()))
        );
        assert_eq!(extracted.len(), 2);
        assert!(
            extracted
                .iter()
                .any(|item| item.name == "TOKEN" && item.env_name.is_none())
        );
        assert!(
            extracted
                .iter()
                .any(|item| item.name == "TOKEN" && item.env_name.as_deref() == Some("ALT"))
        );

        let mut in_args = spec.clone();
        in_args.arguments.push(secret.placeholder.clone());
        assert!(extract_secrets(&secrets, &in_args).is_err());

        let mut partial = spec;
        partial.environment.insert(
            "HDR".into(),
            EnvironmentValue::Literal(format!("Bearer {}", secret.placeholder)),
        );
        assert!(extract_secrets(&secrets, &partial).is_err());
    }

    #[test]
    fn encodes_child_address() {
        assert_eq!(
            folder_list_address("aruna://results/dir?node=peer"),
            "aruna://results/dir/?node=peer"
        );
        assert_eq!(
            child_address("aruna://results/dir", "score%20.csv"),
            "aruna://results/dir/score%2520.csv"
        );
        assert_eq!(
            child_address("aruna://results/dir/?node=peer", "what?#1.csv"),
            "aruna://results/dir/what%3F%231.csv?node=peer"
        );
        let root = Path::new("/scratch/task");
        assert_eq!(
            output_local_path(root, "//tmp/result"),
            Some(root.join(CONTROL).join(wrapper::ROOT).join("tmp/result"))
        );
        assert_eq!(output_local_path(root, "."), Some(root.join(WORK)));
        assert_eq!(
            output_local_path(root, "a\\b.txt"),
            Some(root.join(WORK).join("a\\b.txt"))
        );
        assert_eq!(output_local_path(root, "/"), None);
        assert_eq!(output_local_path(root, "../escape"), None);
    }
}
