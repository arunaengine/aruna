//! Runs the tasks of a workflow run: an attempt is answered from the task cache, or runs as one
//! keyed child job, submitted once or adopted after a restart, and read back from the run's area.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::time::Duration;

use aruna_core::bucket_read::{GetBucketError, GetBucketOperation};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::valid_vault_env;
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::id::NodeId;
use aruna_core::job_request::LABEL_TAG_PREFIX;
use aruna_core::job_request::{SubmissionScope, SubmitJobError, SubmitJobResult};
use aruna_core::jobs::{JobEffect, JobEvent};
use aruna_core::keyspaces::{BUCKET_ENCRYPTION_KEYSPACE, WORKFLOW_TASK_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::stream::BackendStream;
use aruna_core::structs::execution::job::family::{JobProjection, LogicalJobSpec, LogicalJobState};
use aruna_core::structs::execution::job::identity::{JobId, JobState, RETENTION_MS};
use aruna_core::structs::execution::job::input::{
    ComputeResources, ExecutionSpec, InputMode, InputSelection, InputSource, OutputDestination,
    OutputSelection, TaskSecretBinding, WORKFLOW_RUN_TAG, WORKFLOW_SECRETS_TAG, WORKFLOW_TASK_TAG,
    encode_workflow_secrets, run_area_bucket,
};
use aruna_core::structs::execution::job::local::{
    JobRecord, JobResultPayload, OutputObject, WorkspaceMode,
};
use aruna_core::structs::execution::workflow_run::{
    CacheScope, RunFile, RunState, WorkflowRunRecord, WorkflowTaskRecord, named_task, task_key,
};
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::encryption::BucketEncryption;
use aruna_core::structs::storage::node_info::{LOCATION_LABEL_KEY, NODE_LABEL_KEY};
use aruna_core::structs::storage::object_read::ObjectRangeRequest;
use aruna_core::structs::storage::routing::NodeRouting;
use aruna_core::types::Key;
use aruna_ops_data::s3::object::get::{
    GetObjectError, GetObjectInput, GetObjectOperation, GetObjectResult,
};
use aruna_workflow_contract::host::{StartTask, TaskFinished, TaskOutput};
use aruna_workflow_contract::{ArunaStepSettings, ContractError, HostRpcResult};
use tracing::warn;
use ulid::Ulid;

use super::cache::{
    CacheEntry, CacheHit, LookupCacheOperation, StoreCacheOperation, deterministic, task_scope,
};
use super::data::{
    ObjectAccessOperation, Target, WriteBody, WriteObjectOperation, area_of, bucket_answer,
    file_address, resolve,
};
use super::{RunChange, RunTasksOperation, change_call, changed, creator_auth, tasks_answer};
use crate::jobs::job::cancel::{CancelJobOutcome, CancelOwnedOperation, CancelReport};
use crate::jobs::job::reads::JobReadError;
use crate::jobs::lifecycle::admit::here::AcceptedSubmission;
use crate::jobs::lifecycle::cancel::CancelFamilyOperation;
use crate::jobs::lifecycle::family_reads::{FamilyProjectionOperation, FamilyView};
use crate::jobs::lifecycle::submit_external::{
    ExternalSubmission, IngressNode, SubmitExternalOperation,
};
use crate::jobs::local_run::{LocalExecutionConfig, LocalExecutionError, LocalRunOperation};
use crate::jobs::route::JobRouteError;
use crate::jobs::store::mutation_error::JobMutationError;
use crate::jobs::store::row_calls;
use crate::jobs::workflow::prepare::NETWORK_TAG_KEY;
use crate::workflows::CatalogError;

/// How long a task waits between two reads of its child job.
pub const POLL_DELAY: Duration = Duration::from_secs(2);
/// The longest pause before a child refused for quota is sent again.
pub const QUOTA_DELAY_CAP: Duration = Duration::from_secs(60);
/// Bytes of each output stream a finished task keeps.
pub const TAIL_BYTES: u64 = 64 * 1024;
/// The control files the uwe wrapper writes, read back after an attempt.
const CONTROL_FILES: [&str; 6] = ["exit", "start", "end", "manifest", "stdout", "stderr"];

/// The key of uwe's canonical task specification: another key means another task.
pub fn task_key_of(seed: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"aruna-workflow-task-v1\0");
    hasher.update(seed.as_bytes());
    hasher.finalize().to_hex().to_string()
}

/// The idempotency key of one attempt's child job: the run, the task key, the task's place in
/// the run and the attempt, so a repeated call after a restart names the same child.
pub fn child_key(run_id: Ulid, task_key: &str, occurrence: u64, attempt: u32) -> String {
    format!("workflow-task/{run_id}/{task_key}/{occurrence}/{attempt}")
}

/// The number a task has in the run: uwe's, or a stable one derived from the attempt name.
pub fn task_number(request: &StartTask) -> u64 {
    request.task_id.unwrap_or_else(|| named_task(&request.name))
}

/// The folder of one attempt's files in the run's area.
fn attempt_folder(record: &WorkflowRunRecord, name: &str) -> Option<(String, String)> {
    let (bucket, prefix) = area_of(record)?;
    Some((bucket, format!("{prefix}tasks/{name}/")))
}

fn container_path(work: &str, path: &str) -> String {
    match path.starts_with('/') {
        true => path.to_owned(),
        false => format!("{}/{path}", work.trim_end_matches('/')),
    }
}

fn input(path: String, source: InputSource) -> InputSelection {
    InputSelection {
        source,
        source_node_id: None,
        dest_key: path.clone(),
        mode: InputMode::Snapshot,
        container_path: Some(path),
        name: None,
        description: None,
    }
}

fn folder_output(folder: &str, destination: OutputDestination) -> OutputSelection {
    let folder = folder.trim_end_matches('/');
    OutputSelection {
        container_path: format!("{folder}/**"),
        path_prefix: Some(folder.to_owned()),
        destination_node_id: None,
        destination,
        name: None,
        description: None,
    }
}

/// The prefix of an attempt's work folder in the run area of the node that runs it.
fn work_prefix(record: &WorkflowRunRecord, name: &str) -> String {
    format!("{}/tasks/{name}/work", record.run_id)
}

/// The tags that pin a task to a site or a node: the task's own, else its step's, else the
/// run's. They are required labels, so a named site or node never falls back to another.
fn placement_tags(record: &WorkflowRunRecord, request: &StartTask) -> Vec<(String, String)> {
    let settings = request.step_settings.as_ref();
    site_tags(
        request.site.clone(),
        settings,
        record.placement_site.clone(),
    )
}

/// The required label tags of a site the task names, else its step's, else the run's, and of
/// the node its step names.
pub(crate) fn site_tags(
    site: Option<String>,
    settings: Option<&ArunaStepSettings>,
    run_site: Option<String>,
) -> Vec<(String, String)> {
    let placement = settings.map(|settings| &settings.placement);
    let site = site
        .or_else(|| placement.and_then(|placement| placement.site.clone()))
        .or(run_site);
    let node = placement.and_then(|placement| placement.node.clone());
    let label = |key: &str| format!("{LABEL_TAG_PREFIX}{key}");
    let mut tags = Vec::new();
    if let Some(site) = site.filter(|site| !site.trim().is_empty()) {
        tags.push((label(LOCATION_LABEL_KEY), site));
    }
    if let Some(node) = node.filter(|node| !node.trim().is_empty()) {
        tags.push((label(NODE_LABEL_KEY), node));
    }
    tags
}

/// The child job spec of one attempt: the wrapper as an inline file, the direct and staged
/// inputs by their exact versions, and the control and work folders as outputs in the area.
pub fn task_spec(
    record: &WorkflowRunRecord,
    request: &StartTask,
    task_key: &str,
) -> Result<ExecutionSpec, ContractError> {
    let image = (request.image.clone())
        .ok_or_else(|| ContractError::unsupported("the task names no container image"))?;
    let (bucket, folder) = attempt_folder(record, &request.name)
        .ok_or_else(|| ContractError::unsupported("the run names no output bucket"))?;
    let folders = &request.folders;
    let control = folders.control.trim_end_matches('/');
    let script = match control.rsplit_once('/') {
        Some((parent, _)) if !parent.is_empty() => format!("{parent}/task.sh"),
        _ => "/task.sh".to_owned(),
    };
    let mut inputs = vec![input(
        script.clone(),
        InputSource::Inline {
            bytes: request.command_wrapper_bytes.clone(),
        },
    )];
    let mut sources = Vec::new();
    for direct in &request.direct_inputs {
        sources.push((container_path(&folders.work, &direct.path), &direct.address));
    }
    for staged in &request.staged_files {
        if let Some(address) = &staged.address {
            sources.push((container_path(&folders.work, &staged.path), address));
        }
    }
    for (path, address) in sources {
        let target = resolve(record, address)?;
        let source = InputSource::S3 {
            bucket: target.bucket,
            key: target.key,
            version_id: target.version.map(|version| version.to_string()),
        };
        let mut selection = input(path, source);
        selection.source_node_id = target.node;
        inputs.push(selection);
    }
    let mut tags = BTreeMap::from([
        (WORKFLOW_RUN_TAG.to_owned(), record.run_id.to_string()),
        (WORKFLOW_TASK_TAG.to_owned(), task_key.to_owned()),
    ]);
    if request.network_allowed {
        tags.insert(NETWORK_TAG_KEY.to_owned(), "open".to_owned());
    }
    if !request.secret_placeholders.is_empty() {
        let mut bound = BTreeMap::new();
        for placeholder in &request.secret_placeholders {
            let binding =
                record
                    .secrets
                    .get(&placeholder.name)
                    .ok_or_else(|| ContractError::Denied {
                        message: format!("secret `{}` is not bound to this run", placeholder.name),
                    })?;
            let env_name = placeholder.env_name.as_deref().unwrap_or(&placeholder.name);
            if !valid_vault_env(env_name) {
                return Err(ContractError::invalid(format!(
                    "secret environment variable `{env_name}` is not allowed"
                )));
            }
            let entry = TaskSecretBinding {
                workflow_id: record.workflow_id,
                scope: binding.scope,
                name: binding.name.clone(),
                field: binding.field.clone(),
            };
            if let Some(previous) = bound.insert(env_name.to_owned(), entry.clone())
                && previous != entry
            {
                return Err(ContractError::invalid(format!(
                    "secret environment variable `{env_name}` is bound twice"
                )));
            }
        }
        let encoded = encode_workflow_secrets(&bound)
            .map_err(|error| ContractError::internal(error.to_string()))?;
        tags.insert(WORKFLOW_SECRETS_TAG.to_owned(), encoded);
    }
    tags.extend(placement_tags(record, request));
    let resources = &request.resources;
    let work = match record.protected || record.local_only {
        // Protected and local device runs keep work files in the output bucket, never in a realm
        // run area.
        true => OutputDestination::S3 {
            bucket: bucket.clone(),
            key: format!("{folder}work"),
        },
        false => OutputDestination::RunArea {
            prefix: work_prefix(record, &request.name),
        },
    };
    Ok(ExecutionSpec {
        group_id: record.group_id,
        name: Some(request.name.clone()),
        description: Some(format!(
            "step {} of workflow run {}",
            request.step_id, record.run_id
        )),
        tags,
        image,
        entrypoint: Some(vec!["/bin/sh".to_owned(), script]),
        command: Vec::new(),
        workdir: Some(folders.work.clone()),
        env: BTreeMap::new(),
        resources: ComputeResources {
            cpu_cores: (resources.cpus > 0).then_some(resources.cpus),
            ram_bytes: resources.memory_bytes,
            disk_bytes: resources.disk_bytes,
            max_walltime_ms: resources
                .time_seconds
                .map(|seconds| seconds.saturating_mul(1000)),
            preemptible: false,
            gpus: (resources.gpus > 0).then_some(resources.gpus),
        },
        executor_constraint: None,
        backend: None,
        inputs,
        // The control files go to the run's node, which reads them; the work folder of an open
        // run stays in the run area of the node that runs the task. Secret-bearing attempts omit
        // stdout/stderr control files so leaked credentials never reach durable object storage.
        file_outputs: {
            let mut outputs = match request.secret_placeholders.is_empty() {
                true => vec![folder_output(
                    control,
                    OutputDestination::S3 {
                        bucket,
                        key: format!("{folder}control"),
                    },
                )],
                false => ["exit", "start", "end", "manifest"]
                    .into_iter()
                    .map(|name| OutputSelection {
                        container_path: format!("{control}/{name}"),
                        path_prefix: None,
                        destination_node_id: None,
                        destination: OutputDestination::S3 {
                            bucket: bucket.clone(),
                            key: format!("{folder}control/{name}"),
                        },
                        name: None,
                        description: None,
                    })
                    .collect(),
            };
            outputs.push(folder_output(&folders.work, work));
            outputs
        },
        workspace_outputs: Vec::new(),
        output_prefixes: Vec::new(),
        collision_policy: Default::default(),
    })
}

/// What the run records for a task at each turn of its attempt.
fn task_record(
    record: &WorkflowRunRecord,
    request: &StartTask,
    task_key: &str,
    state: RunState,
) -> WorkflowTaskRecord {
    let inputs = (request.direct_inputs.iter())
        .map(|direct| {
            let target = resolve(record, &direct.address).ok();
            RunFile {
                name: direct.path.clone(),
                bucket: target.as_ref().map(|target| target.bucket.clone()),
                key: target.as_ref().map(|target| target.key.clone()),
                version_id: (target.as_ref().and_then(|target| target.version))
                    .map(|version| version.to_string()),
                node_id: target.and_then(|target| target.node),
                size: Some(direct.size),
                digest: Some(direct.digest.clone()),
            }
        })
        .collect();
    WorkflowTaskRecord {
        run_id: record.run_id,
        task_id: task_number(request),
        task_key: task_key.to_owned(),
        step: request.step_id.clone(),
        name: request.name.clone(),
        state,
        attempt: request.attempt,
        index: None,
        child_job_id: None,
        node_id: None,
        mode: None,
        cached: false,
        cache_key: Some(task_key.to_owned()),
        deterministic: Some(deterministic(request)),
        staged_bytes: None,
        cpu_seconds: None,
        max_rss_bytes: None,
        started_at_ms: None,
        finished_at_ms: None,
        exit_code: None,
        error: None,
        inputs,
        outputs: Vec::new(),
        stdout_tail: Vec::new(),
        stderr_tail: Vec::new(),
    }
}

/// Where a child ran and how its inputs reached it: on the node that held them all, or with
/// some of them staged from another node.
#[derive(Clone, Debug, PartialEq)]
struct Placed {
    node_id: NodeId,
    mode: &'static str,
    staged_bytes: u64,
}

impl Placed {
    /// The placement of the child's canonical execution; its captured inputs name the nodes
    /// that held them when the job was admitted.
    fn of(projection: &JobProjection, spec: &LogicalJobSpec) -> Option<Self> {
        let execution = match projection.canonical_execution_id {
            Some(id) => (projection.executions.iter()).find(|run| run.execution_id == id),
            None => projection.executions.last(),
        }?;
        let node_id = execution.executor_node_id;
        let staged = (spec.captured_inputs.iter()).filter(|input| input.source_node_id != node_id);
        let (count, staged_bytes) = staged.fold((0, 0u64), |(count, sum), input| {
            (count + 1, sum + input.bytes)
        });
        Some(Self {
            node_id,
            mode: match count {
                0 => "compute_to_data",
                _ => "data_to_compute",
            },
            staged_bytes,
        })
    }
}

/// A foreign cache hit whose outputs are copied into the run's area one by one.
#[derive(Debug, PartialEq)]
struct Copying {
    hit: CacheHit,
    pending: Vec<RunFile>,
    copied: Vec<RunFile>,
}

/// The terminal facts of a finished child job, whether it ran through the realm or locally.
#[derive(Debug, PartialEq)]
struct ChildSummary {
    job_id: JobId,
    exit_code: Option<i32>,
    stdout: Option<String>,
    stderr: Option<String>,
    outputs: Vec<OutputObject>,
}

impl ChildSummary {
    fn from_projection(projection: &JobProjection) -> Self {
        let result = (projection.canonical_execution_id)
            .and_then(|id| {
                projection
                    .executions
                    .iter()
                    .find(|run| run.execution_id == id)
            })
            .and_then(|run| run.result.as_ref());
        Self {
            job_id: projection.canonical_job_id,
            exit_code: result.and_then(|result| result.exit_code),
            stdout: result
                .and_then(|result| result.stdout.as_ref())
                .map(|tail| tail.as_str().to_owned()),
            stderr: result
                .and_then(|result| result.stderr.as_ref())
                .map(|tail| tail.as_str().to_owned()),
            outputs: projection.outputs.as_slice().to_vec(),
        }
    }

    fn from_local(record: &JobRecord) -> Self {
        let (exit_code, stdout, stderr, outputs) = match &record.result {
            Some(JobResultPayload::Execution {
                exit_code,
                outputs,
                stdout,
                stderr,
                ..
            }) => (
                *exit_code,
                (!stdout.is_empty()).then(|| stdout.clone()),
                (!stderr.is_empty()).then(|| stderr.clone()),
                outputs.clone(),
            ),
            _ => (None, None, None, Vec::new()),
        };
        Self {
            job_id: record.job_id,
            exit_code,
            stdout,
            stderr,
            outputs,
        }
    }
}

#[derive(Debug, PartialEq)]
enum TaskStep {
    Stored,
    Lookup,
    Copy(Box<Copying>, RunFile),
    Queued,
    CheckAccess(Box<ExecutionSpec>, Vec<(String, String)>),
    CheckEncryption(Box<ExecutionSpec>),
    Submit,
    Adopted(JobId),
    Poll(JobId),
    Wait(JobId),
    Started(JobId),
    Control(Box<ChildSummary>),
    Pin(Box<(WorkflowTaskRecord, TaskFinished)>),
    Finished(Box<TaskFinished>),
}

/// Runs one attempt of a task as a keyed child job and answers the coordinator's `StartTask`
/// when the child ended. A child that already exists for the attempt is adopted, never doubled.
#[derive(Debug, PartialEq)]
pub struct RunTaskOperation {
    run: WorkflowRunRecord,
    node: IngressNode,
    routing: NodeRouting,
    request: Box<StartTask>,
    task_key: String,
    scope: CacheScope,
    placed: Option<Placed>,
    waits: u32,
    started: bool,
    now_ms: u64,
    step: TaskStep,
}

type TaskNext = Result<Next<HostRpcResult>, CatalogError>;

impl RunTaskOperation {
    pub fn new(run: WorkflowRunRecord, node: IngressNode, request: Box<StartTask>) -> Self {
        let task_key = task_key_of(&request.task_key_seed);
        let scope = task_scope(&run, &request);
        Self {
            run,
            node,
            routing: NodeRouting::default(),
            request,
            task_key,
            scope,
            placed: None,
            waits: 0,
            started: false,
            now_ms: 0,
            step: TaskStep::Stored,
        }
    }

    /// The node's routing, which a copy of a foreign cache hit writes with.
    pub fn routing(mut self, routing: NodeRouting) -> Self {
        self.routing = routing;
        self
    }

    fn record(&self, state: RunState) -> WorkflowTaskRecord {
        let mut task = task_record(&self.run, &self.request, &self.task_key, state);
        if let Some(placed) = &self.placed {
            task.node_id = Some(placed.node_id);
            task.mode = Some(placed.mode.to_owned());
            task.staged_bytes = Some(placed.staged_bytes);
        }
        task
    }

    /// Queues the attempt for a child job, as no cached result can answer it.
    fn queue(&mut self) -> TaskNext {
        let task = self.record(RunState::Queued);
        self.change(task, TaskStep::Queued)
    }

    fn found(&mut self, hit: Option<CacheHit>) -> TaskNext {
        let Some(hit) = hit else {
            return self.queue();
        };
        if !hit.foreign {
            let outputs = hit.record.outputs.clone();
            return self.reuse(&hit, outputs);
        }
        let pending = hit.record.outputs.iter().rev().cloned().collect();
        let copying = Copying {
            hit,
            pending,
            copied: Vec::new(),
        };
        self.copy_next(copying)
    }

    /// Copies the next output of a foreign hit to where this attempt would have written it.
    fn copy_next(&mut self, mut copying: Copying) -> TaskNext {
        let Some(file) = copying.pending.pop() else {
            let outputs = std::mem::take(&mut copying.copied);
            return self.reuse(&copying.hit, outputs);
        };
        let target = attempt_folder(&self.run, &self.request.name).map(|(bucket, folder)| {
            let key = match file.name.strip_prefix('/') {
                Some(absolute) => format!("{folder}control/root/{absolute}"),
                None => format!("{folder}work/{}", file.name),
            };
            Target {
                bucket,
                key,
                version: None,
                owned: true,
                node: None,
            }
        });
        let version = (file.version_id.as_deref()).and_then(|version| version.parse().ok());
        let (Some(target), Some(bucket), Some(key)) =
            (target, file.bucket.clone(), file.key.clone())
        else {
            return self.queue();
        };
        let source = Target {
            bucket,
            key,
            version,
            owned: false,
            node: file.node_id,
        };
        let write = WriteObjectOperation::new(
            (self.run.clone(), self.run.node_id),
            self.routing.clone(),
            target,
            WriteBody::Copy(source),
        );
        self.step = TaskStep::Copy(Box::new(copying), file);
        Ok(Next::Call(IoCall::child(write)))
    }

    fn copied(&mut self, mut copying: Copying, file: RunFile, result: IoResult) -> TaskNext {
        let receipt = match result.child::<HostRpcResult, CatalogError>()? {
            Ok(HostRpcResult::UploadAck {
                receipt: Some(receipt),
                ..
            }) => receipt,
            // A copy that fails leaves the hit unused; the task runs as if nothing was cached.
            other => {
                warn!(run = %self.run.run_id, task = %self.request.name, ?other, "a cached output was not copied");
                return self.queue();
            }
        };
        let target = resolve(&self.run, &receipt.address).ok();
        copying.copied.push(RunFile {
            name: file.name,
            bucket: target.as_ref().map(|target| target.bucket.clone()),
            key: target.map(|target| target.key),
            version_id: receipt.version,
            node_id: Some(self.run.node_id),
            size: Some(receipt.size),
            digest: file.digest,
        });
        self.copy_next(copying)
    }

    /// Answers the attempt with a cached result. The task record and its events name only the
    /// outputs this run reads, never the run, group or user that made them.
    fn reuse(&mut self, hit: &CacheHit, outputs: Vec<RunFile>) -> TaskNext {
        let mut task = self.record(RunState::Succeeded);
        task.cached = true;
        task.exit_code = Some(hit.record.exit_code);
        task.started_at_ms = Some(self.now_ms);
        task.finished_at_ms = Some(self.now_ms);
        task.stdout_tail = hit.record.stdout_tail.clone();
        task.stderr_tail = hit.record.stderr_tail.clone();
        let answered = (outputs.iter())
            .filter_map(|file| {
                Some(TaskOutput {
                    path: file.name.clone(),
                    directory: false,
                    address: file_address(self.run.node_id, file)?,
                    digest: None,
                    size: file.size.unwrap_or_default(),
                })
            })
            .collect();
        task.outputs = outputs;
        let finished = TaskFinished {
            exit_code: Some(hit.record.exit_code),
            outputs: answered,
            stdout_tail: hit.record.stdout_tail.clone(),
            stderr_tail: hit.record.stderr_tail.clone(),
            ..TaskFinished::default()
        };
        self.change(task, TaskStep::Finished(Box::new(finished)))
    }

    /// Keeps the result of a successful attempt in the cache of its scope, with its outputs
    /// pinned to the versions it wrote.
    fn keep(&mut self, task: WorkflowTaskRecord, finished: TaskFinished) -> TaskNext {
        let entry = CacheEntry {
            task_key: self.task_key.clone(),
            outputs: task.outputs.clone(),
            stdout_tail: task.stdout_tail.clone(),
            stderr_tail: task.stderr_tail.clone(),
            deterministic: deterministic(&self.request),
            has_secrets: !self.request.secret_placeholders.is_empty(),
        };
        let store = StoreCacheOperation::new(&self.run, self.scope, entry);
        self.step = TaskStep::Pin(Box::new((task, finished)));
        Ok(Next::Call(IoCall::child(store)))
    }

    fn pinned(
        &mut self,
        (mut task, mut finished): (WorkflowTaskRecord, TaskFinished),
        result: IoResult,
    ) -> TaskNext {
        match result.child::<Vec<RunFile>, CatalogError>()? {
            Ok(pinned) => {
                for output in &mut finished.outputs {
                    let file = pinned.iter().find(|file| file.name == output.path);
                    if let Some(file) = file.filter(|file| file.version_id.is_some()) {
                        if let Some(address) = file_address(self.run.node_id, file) {
                            output.address = address;
                        }
                        output.size = file.size.unwrap_or(output.size);
                    }
                }
                task.outputs = pinned;
            }
            Err(error) => {
                warn!(%error, run = %self.run.run_id, task = %self.request.name, "a task result was not cached");
            }
        }
        self.change(task, TaskStep::Finished(Box::new(finished)))
    }

    fn change(&mut self, task: WorkflowTaskRecord, step: TaskStep) -> TaskNext {
        self.step = step;
        let change = RunChange::Task(Box::new(task));
        Ok(Next::Call(change_call(self.run.run_id, vec![change])))
    }

    /// Ends the attempt without a child: the coordinator decides whether to try again.
    fn refuse(&mut self, error: String, retryable: bool) -> TaskNext {
        let mut task = self.record(RunState::Failed);
        task.error = Some(error.clone());
        task.finished_at_ms = Some(self.now_ms);
        let finished = TaskFinished {
            error: Some(error),
            retryable,
            ..TaskFinished::default()
        };
        self.change(task, TaskStep::Finished(Box::new(finished)))
    }

    fn submit(&mut self) -> TaskNext {
        let spec = match task_spec(&self.run, &self.request, &self.task_key) {
            Ok(spec) => spec,
            Err(error) => return self.refuse(error.to_string(), false),
        };
        if self.run.path_restrictions.is_some() {
            let local = self.run.node_id;
            let area = area_of(&self.run);
            let mut checks = Vec::new();
            for item in &spec.inputs {
                let InputSource::S3 { bucket, key, .. } = &item.source else {
                    continue;
                };
                if item.source_node_id.is_some_and(|node| node != local) {
                    return self.refuse(
                        "a restricted run may not read inputs from another node".into(),
                        false,
                    );
                }
                let owned = area
                    .as_ref()
                    .is_some_and(|(ab, ap)| ab == bucket && key.starts_with(ap));
                if !owned {
                    checks.push((bucket.clone(), key.clone()));
                }
            }
            if !checks.is_empty() {
                return self.check_access(Box::new(spec), checks);
            }
        }
        self.check_encryption(Box::new(spec))
    }

    fn check_access(
        &mut self,
        spec: Box<ExecutionSpec>,
        mut pending: Vec<(String, String)>,
    ) -> TaskNext {
        let Some((bucket, key)) = pending.pop() else {
            return self.check_encryption(spec);
        };
        let access = ObjectAccessOperation::new(
            &creator_auth(&self.run),
            self.run.node_id,
            (&bucket, &key),
            Permission::READ,
        );
        self.step = TaskStep::CheckAccess(spec, pending);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn check_encryption(&mut self, spec: Box<ExecutionSpec>) -> TaskNext {
        if self.run.protected {
            return self.submit_spec(*spec);
        }
        let local = self.run.node_id;
        let area = area_of(&self.run);
        let mut buckets: Vec<String> = spec
            .inputs
            .iter()
            .filter_map(|item| match &item.source {
                InputSource::S3 { bucket, key, .. }
                    if item.source_node_id.is_none_or(|n| n == local)
                        && !area
                            .as_ref()
                            .is_some_and(|(ab, ap)| ab == bucket && key.starts_with(ap)) =>
                {
                    Some(bucket.clone())
                }
                _ => None,
            })
            .collect();
        buckets.sort_unstable();
        buckets.dedup();
        if buckets.is_empty() {
            return self.submit_spec(*spec);
        }
        let rows = buckets
            .into_iter()
            .map(|bucket| {
                (
                    BUCKET_ENCRYPTION_KEYSPACE.to_string(),
                    Key::from(bucket.into_bytes()),
                )
            })
            .collect();
        self.step = TaskStep::CheckEncryption(spec);
        Ok(Next::Call(IoCall::Read(rows)))
    }

    fn submit_spec(&mut self, spec: ExecutionSpec) -> TaskNext {
        let occurrence = task_number(&self.request);
        let key = child_key(
            self.run.run_id,
            &self.task_key,
            occurrence,
            self.request.attempt,
        );
        if self.run.local_only {
            let local = LocalRunOperation::new(
                LocalExecutionConfig {
                    spec,
                    owner: self.run.created_by,
                    node_id: self.run.node_id,
                    idempotency_key: Some(key),
                    workspace_mode: WorkspaceMode::None,
                    retention_ms: RETENTION_MS,
                },
                None,
            );
            self.step = TaskStep::Submit;
            return Ok(Next::Call(IoCall::child(local)));
        }
        let submission = ExternalSubmission {
            spec,
            created_by: self.run.created_by,
            scope: SubmissionScope::Keyed(key),
            workspace_mode: WorkspaceMode::None,
            workspace_bucket: None,
            retention_ms: RETENTION_MS,
            auth_token: None,
        };
        let mut node = self.node.clone();
        node.now_ms = self.now_ms.max(node.now_ms);
        let submit = SubmitExternalOperation::new(submission, Some(node))
            .with_precaptured(self.run.captured_remote.clone());
        self.step = TaskStep::Submit;
        Ok(Next::Call(IoCall::child(submit)))
    }

    fn submitted(&mut self, result: IoResult) -> TaskNext {
        if self.run.local_only {
            return self.submitted_local(result);
        }
        let job_id = match result.child::<AcceptedSubmission, SubmitJobError>()? {
            Ok(accepted) => accepted.job_id,
            // Another attempt of this node submitted the child already; it is the same task.
            Err(SubmitJobError::JobPlanConflict { existing_job_id }) => existing_job_id,
            Err(SubmitJobError::QuotaDenied(denied)) => {
                // A refused quota is a wait, never a failure of the task.
                self.waits += 1;
                let delay = POLL_DELAY.saturating_mul(1 << self.waits.min(5));
                warn!(run = %self.run.run_id, task = %self.request.name, %denied, "waiting for quota");
                self.step = TaskStep::Queued;
                return Ok(Next::Call(IoCall::Delay(delay.min(QUOTA_DELAY_CAP))));
            }
            Err(
                error @ (SubmitJobError::InvalidWorkspace(_)
                | SubmitJobError::AuthorityDenied
                | SubmitJobError::InputDenied(_)),
            ) => return self.refuse(error.to_string(), false),
            Err(error) => return self.refuse(error.to_string(), true),
        };
        let mut task = self.record(RunState::Queued);
        task.child_job_id = Some(job_id);
        self.change(task, TaskStep::Adopted(job_id))
    }

    fn submitted_local(&mut self, result: IoResult) -> TaskNext {
        let job_id = match result.child::<SubmitJobResult, LocalExecutionError>()? {
            Ok(submitted) => submitted.job_id,
            Err(LocalExecutionError::Submit(SubmitJobError::JobPlanConflict {
                existing_job_id,
            })) => existing_job_id,
            Err(LocalExecutionError::Submit(SubmitJobError::ActiveJobLimit { limit })) => {
                self.waits += 1;
                let delay = POLL_DELAY.saturating_mul(1 << self.waits.min(5));
                warn!(run = %self.run.run_id, task = %self.request.name, limit, "waiting for local slot");
                self.step = TaskStep::Queued;
                return Ok(Next::Call(IoCall::Delay(delay.min(QUOTA_DELAY_CAP))));
            }
            Err(
                error @ (LocalExecutionError::NotADevice
                | LocalExecutionError::NotOwner
                | LocalExecutionError::Unsupported(_)
                | LocalExecutionError::Refused(_)
                | LocalExecutionError::InputNotLocal { .. }
                | LocalExecutionError::InputRefused { .. }),
            ) => return self.refuse(error.to_string(), false),
            Err(error) => return self.refuse(error.to_string(), true),
        };
        let mut task = self.record(RunState::Queued);
        task.child_job_id = Some(job_id);
        self.change(task, TaskStep::Adopted(job_id))
    }

    fn poll(&mut self, job_id: JobId) -> TaskNext {
        self.step = TaskStep::Poll(job_id);
        if self.run.local_only {
            return Ok(Next::Call(IoCall::child(row_calls::read_job_record(
                job_id,
            ))));
        }
        let read =
            FamilyProjectionOperation::new(job_id, self.now_ms).for_reader(self.run.created_by);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn polled(&mut self, job_id: JobId, result: IoResult) -> TaskNext {
        if self.run.local_only {
            return self.polled_local(job_id, result);
        }
        let view = match result.child::<Option<FamilyView>, JobRouteError>()? {
            Ok(view) => view,
            Err(error) => {
                warn!(%error, run = %self.run.run_id, "a child job could not be read; waiting");
                None
            }
        };
        let Some((projection, spec)) =
            view.and_then(|(family, spec)| Some((family.projection?, spec)))
        else {
            return self.wait(job_id);
        };
        if let Some(placed) = Placed::of(&projection, &spec) {
            self.placed = Some(placed);
        }
        match projection.state {
            LogicalJobState::Queued | LogicalJobState::Indeterminate => self.wait(job_id),
            LogicalJobState::Running if self.started => self.wait(job_id),
            LogicalJobState::Running => {
                self.started = true;
                let mut task = self.record(RunState::Running);
                task.child_job_id = Some(job_id);
                task.started_at_ms = Some(self.now_ms);
                self.change(task, TaskStep::Started(job_id))
            }
            LogicalJobState::Cancelled => {
                let mut task = self.record(RunState::Cancelled);
                task.child_job_id = Some(job_id);
                task.finished_at_ms = Some(self.now_ms);
                let finished = TaskFinished {
                    cancelled: true,
                    ..TaskFinished::default()
                };
                self.change(task, TaskStep::Finished(Box::new(finished)))
            }
            LogicalJobState::NotRun => {
                self.refuse("the node that ran the task left the realm".into(), true)
            }
            LogicalJobState::Succeeded | LogicalJobState::Failed => {
                self.read_control(ChildSummary::from_projection(&projection))
            }
        }
    }

    fn polled_local(&mut self, job_id: JobId, result: IoResult) -> TaskNext {
        let record = match result.child::<Option<JobRecord>, JobMutationError>()? {
            Ok(Some(record)) => record,
            Ok(None) => return self.wait(job_id),
            Err(error) => {
                warn!(%error, run = %self.run.run_id, "a local child job could not be read; waiting");
                return self.wait(job_id);
            }
        };
        self.placed = Some(Placed {
            node_id: self.run.node_id,
            mode: "local",
            staged_bytes: 0,
        });
        match record.state {
            JobState::Queued
            | JobState::Claimed
            | JobState::Preparing
            | JobState::Ready
            | JobState::Cancelling
            | JobState::Indeterminate
            | JobState::AwaitingKey => self.wait(job_id),
            JobState::Running if self.started => self.wait(job_id),
            JobState::Running => {
                self.started = true;
                let mut task = self.record(RunState::Running);
                task.child_job_id = Some(job_id);
                task.started_at_ms = Some(self.now_ms);
                self.change(task, TaskStep::Started(job_id))
            }
            JobState::Cancelled => {
                let mut task = self.record(RunState::Cancelled);
                task.child_job_id = Some(job_id);
                task.finished_at_ms = Some(self.now_ms);
                let finished = TaskFinished {
                    cancelled: true,
                    ..TaskFinished::default()
                };
                self.change(task, TaskStep::Finished(Box::new(finished)))
            }
            JobState::NotRun => self.refuse("the local child job could not run".into(), true),
            JobState::Succeeded | JobState::Failed => {
                self.read_control(ChildSummary::from_local(&record))
            }
        }
    }

    fn read_control(&mut self, summary: ChildSummary) -> TaskNext {
        let (bucket, folder) = attempt_folder(&self.run, &self.request.name)
            .ok_or_else(|| CatalogError::Invalid("the run names no output bucket".into()))?;
        let read = ControlFilesOperation::new(&self.run, bucket, format!("{folder}control/"));
        self.step = TaskStep::Control(Box::new(summary));
        Ok(Next::Call(IoCall::child(read)))
    }

    fn wait(&mut self, job_id: JobId) -> TaskNext {
        self.step = TaskStep::Wait(job_id);
        Ok(Next::Call(IoCall::Delay(POLL_DELAY)))
    }

    /// Where an output the manifest names lies: an absolute path in the control folder on the
    /// run's node, a relative one in the run area of the node that ran the task. The child's
    /// output records give the exact version, its size and digest.
    fn output_file(&self, summary: &ChildSummary, path: &str) -> RunFile {
        let (bucket, folder) = attempt_folder(&self.run, &self.request.name).unwrap_or_default();
        let (bucket, key, node) = match path.strip_prefix('/') {
            Some(absolute) => {
                let key = format!("{folder}control/root/{absolute}");
                (bucket, key, self.run.node_id)
            }
            None if self.run.protected || self.run.local_only => {
                let key = format!("{folder}work/{path}");
                (bucket, key, self.run.node_id)
            }
            None => {
                let node = (self.placed.as_ref()).map_or(self.run.node_id, |placed| placed.node_id);
                let bucket = run_area_bucket(self.run.group_id, node);
                let key = format!("{}/{path}", work_prefix(&self.run, &self.request.name));
                (bucket, key, node)
            }
        };
        let written =
            (summary.outputs.iter()).find(|object| object.bucket == bucket && object.key == key);
        RunFile {
            name: path.to_owned(),
            bucket: Some(bucket),
            key: Some(key),
            version_id: written.map(|object| object.version_id.to_string()),
            node_id: Some(written.map_or(node, |object| object.node_id)),
            size: written.map(|object| object.size),
            digest: written.and_then(|object| object.digest.clone()),
        }
    }

    /// The answer of an ended child, from the wrapper's control files where they exist and the
    /// child's own result where they do not.
    fn collected(&mut self, summary: ChildSummary, files: BTreeMap<String, Vec<u8>>) -> TaskNext {
        let text = |name: &str| {
            files
                .get(name)
                .map(|bytes| String::from_utf8_lossy(bytes).trim().to_owned())
        };
        let exit_code = text("exit")
            .and_then(|code| code.parse().ok())
            .or(summary.exit_code);
        let tail = |name: &str, fallback: Option<&String>| {
            files
                .get(name)
                .cloned()
                .unwrap_or_else(|| fallback.cloned().unwrap_or_default().into_bytes())
        };
        let manifest: Vec<(String, bool)> = (text("manifest").unwrap_or_default().lines())
            .filter_map(|line| line.split_once(' '))
            .filter(|(kind, path)| {
                matches!(*kind, "file" | "directory")
                    && !path.is_empty()
                    && !path.contains('\0')
                    && !path.split('/').any(|part| part == "..")
            })
            .map(|(kind, path)| (path.to_owned(), kind == "directory"))
            .collect();
        let files: Vec<RunFile> = (manifest.iter())
            .map(|(path, _)| self.output_file(&summary, path))
            .collect();
        let outputs = (files.iter().zip(&manifest))
            .map(|(file, (_, directory))| TaskOutput {
                path: file.name.clone(),
                directory: *directory,
                address: file_address(self.run.node_id, file).unwrap_or_default(),
                digest: None,
                size: file.size.unwrap_or_default(),
            })
            .collect();
        let seconds = |name: &str| text(name).and_then(|value| value.parse::<f64>().ok());
        let has_secrets = !self.request.secret_placeholders.is_empty();
        let finished = TaskFinished {
            exit_code,
            error: None,
            retryable: false,
            cancelled: false,
            outputs,
            stdout_tail: match has_secrets {
                true => Vec::new(),
                false => tail("stdout", summary.stdout.as_ref()),
            },
            stderr_tail: match has_secrets {
                true => Vec::new(),
                false => tail("stderr", summary.stderr.as_ref()),
            },
            started: seconds("start"),
            ended: seconds("end"),
            usage: None,
        };
        let state = match exit_code {
            Some(0) => RunState::Succeeded,
            _ => RunState::Failed,
        };
        let mut task = self.record(state);
        task.child_job_id = Some(summary.job_id);
        task.exit_code = exit_code;
        task.started_at_ms = finished.started.map(|seconds| (seconds * 1000.0) as u64);
        task.finished_at_ms = Some(self.now_ms);
        task.stdout_tail = finished.stdout_tail.clone();
        task.stderr_tail = finished.stderr_tail.clone();
        task.outputs = files;
        // Only a result with exit code 0 is kept; another one is never reused.
        if exit_code == Some(0) {
            return self.keep(task, finished);
        }
        self.change(task, TaskStep::Finished(Box::new(finished)))
    }
}

impl Operation for RunTaskOperation {
    type Output = HostRpcResult;
    type Error = CatalogError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> TaskNext {
        let key = task_key(self.run.run_id, task_number(&self.request));
        self.step = TaskStep::Stored;
        Ok(Next::Call(IoCall::Read(vec![(
            WORKFLOW_TASK_KEYSPACE.to_string(),
            key,
        )])))
    }

    fn next(&mut self, result: IoResult) -> TaskNext {
        match std::mem::replace(&mut self.step, TaskStep::Queued) {
            TaskStep::Stored => {
                let stored = (result
                    .value()?
                    .map(|value| WorkflowTaskRecord::from_bytes(&value)))
                .transpose()?;
                // An earlier attempt of this node submitted the child: adopt it.
                let adopted = stored
                    .filter(|task| task.attempt == self.request.attempt)
                    .and_then(|task| task.child_job_id);
                match adopted {
                    Some(job_id) => self.poll(job_id),
                    None => {
                        let lookup =
                            LookupCacheOperation::new(&self.run, self.scope, &self.task_key);
                        self.step = TaskStep::Lookup;
                        Ok(Next::Call(IoCall::child(lookup)))
                    }
                }
            }
            TaskStep::Lookup => match result.child::<Option<CacheHit>, CatalogError>()? {
                Ok(hit) => self.found(hit),
                Err(error) => {
                    warn!(%error, run = %self.run.run_id, "the task cache was not read");
                    self.queue()
                }
            },
            TaskStep::Copy(copying, file) => self.copied(*copying, file, result),
            TaskStep::Pin(parts) => self.pinned(*parts, result),
            TaskStep::Queued => {
                if !matches!(result, IoResult::Woke) {
                    changed(result)?;
                }
                self.submit()
            }
            TaskStep::CheckAccess(spec, pending) => match bucket_answer(result) {
                Ok(_) => self.check_access(spec, pending),
                Err(CatalogError::Forbidden | CatalogError::NotFound) => {
                    self.refuse("the run's creator may not read a task input".into(), false)
                }
                Err(error) => Err(error),
            },
            TaskStep::CheckEncryption(spec) => {
                for row in result.values()? {
                    let settings = BucketEncryption::from_row(row.as_deref())
                        .map_err(|error| CatalogError::Storage(error.to_string()))?;
                    if settings.is_encrypted() {
                        return self.refuse(
                            "encrypted inputs require a protected workflow run".into(),
                            false,
                        );
                    }
                }
                self.submit_spec(*spec)
            }
            TaskStep::Submit => self.submitted(result),
            TaskStep::Adopted(job_id) | TaskStep::Started(job_id) => {
                changed(result)?;
                self.poll(job_id)
            }
            TaskStep::Wait(job_id) => match result {
                IoResult::Woke => self.poll(job_id),
                other => Err(IoError::Unexpected {
                    expected: "IoResult::Woke",
                    got: other.label(),
                }
                .into()),
            },
            TaskStep::Poll(job_id) => self.polled(job_id, result),
            TaskStep::Control(projection) => {
                let files = result.child::<BTreeMap<String, Vec<u8>>, CatalogError>()??;
                self.collected(*projection, files)
            }
            TaskStep::Finished(finished) => {
                changed(result)?;
                Ok(Next::Done(HostRpcResult::TaskFinished(finished)))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum ControlStep {
    Bucket,
    Get(&'static str),
    Collect(&'static str, Box<GetObjectResult>),
}

/// Reads the wrapper's control files of one attempt from the run's area; a missing file is left
/// out, and the two output streams keep only their tails.
#[derive(Debug, PartialEq)]
pub struct ControlFilesOperation {
    run_id: Ulid,
    created_by: aruna_core::user::id::UserId,
    realm_id: aruna_core::structs::identity::realm::RealmId,
    node_id: NodeId,
    bucket: String,
    folder: String,
    pending: Vec<&'static str>,
    group: Option<BucketInfo>,
    files: BTreeMap<String, Vec<u8>>,
    step: ControlStep,
}

impl ControlFilesOperation {
    pub fn new(run: &WorkflowRunRecord, bucket: String, folder: String) -> Self {
        Self {
            run_id: run.run_id,
            created_by: run.created_by,
            realm_id: run.realm_id,
            node_id: run.node_id,
            bucket,
            folder,
            pending: CONTROL_FILES.iter().rev().copied().collect(),
            group: None,
            files: BTreeMap::new(),
            step: ControlStep::Bucket,
        }
    }

    fn get_next(&mut self) -> Result<Next<BTreeMap<String, Vec<u8>>>, CatalogError> {
        let Some(name) = self.pending.pop() else {
            return Ok(Next::Done(std::mem::take(&mut self.files)));
        };
        let group = self.group.as_ref().ok_or(IoError::Unfinished)?.group_id;
        let range = matches!(name, "stdout" | "stderr")
            .then_some(ObjectRangeRequest::Suffix { length: TAIL_BYTES });
        let get = GetObjectOperation::new(GetObjectInput {
            bucket: self.bucket.clone(),
            key: format!("{}{name}", self.folder),
            version_id: None,
            range,
            group_id: group,
            user_identity: self.created_by,
            realm_id: self.realm_id,
            node_id: self.node_id,
        });
        self.step = ControlStep::Get(name);
        Ok(Next::Call(IoCall::child(get)))
    }
}

impl Operation for ControlFilesOperation {
    type Output = BTreeMap<String, Vec<u8>>;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<Self::Output>, CatalogError> {
        Ok(Next::Call(IoCall::child(GetBucketOperation::new(
            self.bucket.clone(),
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, CatalogError> {
        match std::mem::replace(&mut self.step, ControlStep::Bucket) {
            ControlStep::Bucket => {
                let info = match result.child::<BucketInfo, GetBucketError>()? {
                    Ok(info) => info,
                    Err(GetBucketError::NotFound) => return Ok(Next::Done(BTreeMap::new())),
                    Err(error) => return Err(CatalogError::Storage(error.to_string())),
                };
                self.group = Some(info);
                self.get_next()
            }
            ControlStep::Get(name) => match result.child::<GetObjectResult, GetObjectError>()? {
                Ok(mut object) => {
                    let stream = std::mem::replace(&mut object.blob, BackendStream::empty());
                    let collect = JobEffect::CollectUpTo {
                        stream,
                        limit: TAIL_BYTES,
                    };
                    self.step = ControlStep::Collect(name, Box::new(object));
                    Ok(Next::Call(IoCall::Effect(Box::new(Effect::Job(collect)))))
                }
                Err(
                    GetObjectError::NoSuchKey
                    | GetObjectError::NoSuchVersion
                    | GetObjectError::InvalidRange,
                ) => self.get_next(),
                Err(error) => {
                    warn!(%error, run = %self.run_id, file = name, "a control file was not read");
                    self.get_next()
                }
            },
            ControlStep::Collect(name, _) => {
                match result.event()? {
                    Event::Job(JobEvent::CollectedUpTo(Ok(Some(bytes)))) => {
                        self.files.insert(name.to_owned(), bytes);
                    }
                    Event::Job(JobEvent::CollectedUpTo(_)) => {}
                    _ => return Err(crate::workflows::unexpected("JobEvent::CollectedUpTo")),
                }
                self.get_next()
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum StopStep {
    Tasks,
    Cancel(String, Vec<WorkflowTaskRecord>),
    LocalCancel(String, Vec<WorkflowTaskRecord>),
}

/// Cancels child jobs of a run that have not ended: one named attempt for `CancelTask`, or all
/// of them for `StopOrphans`. Local device runs cancel owned jobs; realm runs cancel families.
#[derive(Debug, PartialEq)]
pub struct StopTasksOperation {
    run: WorkflowRunRecord,
    serving: (NodeId, aruna_core::structs::identity::realm::RealmId),
    name: Option<String>,
    stopped: Vec<String>,
    failed: bool,
    now_ms: u64,
    step: StopStep,
}

impl StopTasksOperation {
    /// Stops the attempt `name`, or every running child when `name` is `None`.
    pub fn new(
        run: WorkflowRunRecord,
        serving: (NodeId, aruna_core::structs::identity::realm::RealmId),
        name: Option<String>,
    ) -> Self {
        Self {
            run,
            serving,
            name,
            stopped: Vec::new(),
            failed: false,
            now_ms: 0,
            step: StopStep::Tasks,
        }
    }

    fn cancel_next(&mut self, mut tasks: Vec<WorkflowTaskRecord>) -> TaskNext {
        let Some(task) = tasks.pop() else {
            let stopped = std::mem::take(&mut self.stopped);
            return Ok(Next::Done(match self.name.take() {
                Some(_) if self.failed => HostRpcResult::Failed(ContractError::internal(
                    "the child task could not be cancelled",
                )),
                Some(name) => HostRpcResult::TaskCancelled { name },
                None => HostRpcResult::StopOrphans { stopped },
            }));
        };
        let Some(job_id) = task.child_job_id else {
            return self.cancel_next(tasks);
        };
        if self.run.local_only {
            let cancel = CancelOwnedOperation::new(self.run.created_by, job_id, self.now_ms);
            self.step = StopStep::LocalCancel(task.name, tasks);
            return Ok(Next::Call(IoCall::child(cancel)));
        }
        let cancel = CancelFamilyOperation::new(
            &creator_auth(&self.run),
            job_id,
            None,
            Some(self.serving),
            self.now_ms,
        );
        self.step = StopStep::Cancel(task.name, tasks);
        Ok(Next::Call(IoCall::child(cancel)))
    }
}

impl Operation for StopTasksOperation {
    type Output = HostRpcResult;
    type Error = CatalogError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> TaskNext {
        Ok(Next::Call(IoCall::child(RunTasksOperation::new(
            self.run.run_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> TaskNext {
        match std::mem::replace(&mut self.step, StopStep::Tasks) {
            StopStep::Tasks => {
                let name = self.name.clone();
                let tasks = (tasks_answer(result)?.into_iter())
                    .filter(|task| !task.state.is_terminal())
                    .filter(|task| name.as_ref().is_none_or(|name| *name == task.name))
                    .collect();
                self.cancel_next(tasks)
            }
            StopStep::Cancel(name, tasks) => {
                match result.child::<Option<()>, JobRouteError>()? {
                    Ok(Some(())) => self.stopped.push(name),
                    Ok(None) => {
                        self.failed = true;
                        warn!(run = %self.run.run_id, task = %name, "child job family not found");
                    }
                    Err(error) => {
                        self.failed = true;
                        warn!(%error, run = %self.run.run_id, task = %name, "a child job was not cancelled");
                    }
                }
                self.cancel_next(tasks)
            }
            StopStep::LocalCancel(name, tasks) => {
                match result.child::<CancelReport<CancelJobOutcome>, JobReadError>()? {
                    Ok(report) => match report.outcome {
                        CancelJobOutcome::Requested(_) | CancelJobOutcome::AlreadyTerminal(_) => {
                            self.stopped.push(name);
                        }
                        CancelJobOutcome::NotFound => {
                            self.failed = true;
                            warn!(run = %self.run.run_id, task = %name, "local child job not found");
                        }
                    },
                    Err(error) => {
                        self.failed = true;
                        warn!(%error, run = %self.run.run_id, task = %name, "local child job was not cancelled");
                    }
                }
                self.cancel_next(tasks)
            }
        }
    }
}

#[cfg(test)]
#[path = "place_tests.rs"]
mod place_tests;
