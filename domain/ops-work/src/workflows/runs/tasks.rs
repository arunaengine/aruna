//! Runs the tasks of a workflow run as keyed child execution jobs: one attempt is one child,
//! submitted once or adopted after a restart, waited for, and read back from the run's area.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::time::Duration;

use aruna_core::bucket_read::{GetBucketError, GetBucketOperation};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::id::NodeId;
use aruna_core::job_request::{SubmissionScope, SubmitJobError};
use aruna_core::jobs::{JobEffect, JobEvent};
use aruna_core::keyspaces::WORKFLOW_TASK_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::stream::BackendStream;
use aruna_core::structs::execution::job::family::{JobProjection, LogicalJobState};
use aruna_core::structs::execution::job::identity::{JobId, RETENTION_MS};
use aruna_core::structs::execution::job::input::{
    ComputeResources, ExecutionSpec, InputMode, InputSelection, InputSource, OutputDestination,
    OutputSelection, WORKFLOW_RUN_TAG, WORKFLOW_TASK_TAG,
};
use aruna_core::structs::execution::job::local::WorkspaceMode;
use aruna_core::structs::execution::workflow_run::{
    RunFile, RunState, WorkflowRunRecord, WorkflowTaskRecord, named_task, task_key,
};
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::object_read::ObjectRangeRequest;
use aruna_ops_data::s3::object::get::{
    GetObjectError, GetObjectInput, GetObjectOperation, GetObjectResult,
};
use aruna_workflow_contract::host::{StartTask, TaskFinished, TaskOutput};
use aruna_workflow_contract::{ContractError, HostRpcResult};
use tracing::warn;
use ulid::Ulid;

use super::data::{address_of, area_of, resolve};
use super::{RunChange, RunTasksOperation, change_call, changed, creator_auth, tasks_answer};
use crate::jobs::lifecycle::admit::here::AcceptedSubmission;
use crate::jobs::lifecycle::cancel::CancelFamilyOperation;
use crate::jobs::lifecycle::family_reads::{FamilyProjectionOperation, FamilyView};
use crate::jobs::lifecycle::submit_external::{
    ExternalSubmission, IngressNode, SubmitExternalOperation,
};
use crate::jobs::route::JobRouteError;
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

fn folder_output(folder: &str, bucket: &str, key: &str) -> OutputSelection {
    let folder = folder.trim_end_matches('/');
    OutputSelection {
        container_path: format!("{folder}/**"),
        path_prefix: Some(folder.to_owned()),
        destination_node_id: None,
        destination: OutputDestination::S3 {
            bucket: bucket.to_owned(),
            key: key.trim_end_matches('/').to_owned(),
        },
        name: None,
        description: None,
    }
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
        inputs.push(input(path, source));
    }
    let mut tags = BTreeMap::from([
        (WORKFLOW_RUN_TAG.to_owned(), record.run_id.to_string()),
        (WORKFLOW_TASK_TAG.to_owned(), task_key.to_owned()),
    ]);
    if request.network_allowed {
        tags.insert(NETWORK_TAG_KEY.to_owned(), "open".to_owned());
    }
    let resources = &request.resources;
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
        },
        executor_constraint: None,
        backend: None,
        inputs,
        file_outputs: vec![
            folder_output(control, &bucket, &format!("{folder}control")),
            folder_output(&folders.work, &bucket, &format!("{folder}work")),
        ],
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
                version_id: (target.and_then(|target| target.version))
                    .map(|version| version.to_string()),
                node_id: None,
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
        node_id: Some(record.node_id),
        mode: Some("compute_to_data".to_owned()),
        cached: false,
        cache_key: Some(task_key.to_owned()),
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

#[derive(Debug, PartialEq)]
enum TaskStep {
    Stored,
    Queued,
    Submit,
    Adopted(JobId),
    Poll(JobId),
    Wait(JobId),
    Started(JobId),
    Control(Box<JobProjection>),
    Finished(Box<TaskFinished>),
}

/// Runs one attempt of a task as a keyed child job and answers the coordinator's `StartTask`
/// when the child ended. A child that already exists for the attempt is adopted, never doubled.
#[derive(Debug, PartialEq)]
pub struct RunTaskOperation {
    run: WorkflowRunRecord,
    node: IngressNode,
    request: Box<StartTask>,
    task_key: String,
    waits: u32,
    started: bool,
    now_ms: u64,
    step: TaskStep,
}

type TaskNext = Result<Next<HostRpcResult>, CatalogError>;

impl RunTaskOperation {
    pub fn new(run: WorkflowRunRecord, node: IngressNode, request: Box<StartTask>) -> Self {
        let task_key = task_key_of(&request.task_key_seed);
        Self {
            run,
            node,
            request,
            task_key,
            waits: 0,
            started: false,
            now_ms: 0,
            step: TaskStep::Stored,
        }
    }

    fn record(&self, state: RunState) -> WorkflowTaskRecord {
        task_record(&self.run, &self.request, &self.task_key, state)
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
        let occurrence = task_number(&self.request);
        let key = child_key(
            self.run.run_id,
            &self.task_key,
            occurrence,
            self.request.attempt,
        );
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
        let submit = SubmitExternalOperation::new(submission, Some(node));
        self.step = TaskStep::Submit;
        Ok(Next::Call(IoCall::child(submit)))
    }

    fn submitted(&mut self, result: IoResult) -> TaskNext {
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

    fn poll(&mut self, job_id: JobId) -> TaskNext {
        let read =
            FamilyProjectionOperation::new(job_id, self.now_ms).for_reader(self.run.created_by);
        self.step = TaskStep::Poll(job_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn polled(&mut self, job_id: JobId, result: IoResult) -> TaskNext {
        let view = match result.child::<Option<FamilyView>, JobRouteError>()? {
            Ok(view) => view,
            Err(error) => {
                warn!(%error, run = %self.run.run_id, "a child job could not be read; waiting");
                None
            }
        };
        let projection = view.and_then(|(family, _)| family.projection);
        let Some(projection) = projection else {
            return self.wait(job_id);
        };
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
                let (bucket, folder) =
                    attempt_folder(&self.run, &self.request.name).ok_or_else(|| {
                        CatalogError::Invalid("the run names no output bucket".into())
                    })?;
                let read =
                    ControlFilesOperation::new(&self.run, bucket, format!("{folder}control/"));
                self.step = TaskStep::Control(Box::new(projection));
                Ok(Next::Call(IoCall::child(read)))
            }
        }
    }

    fn wait(&mut self, job_id: JobId) -> TaskNext {
        self.step = TaskStep::Wait(job_id);
        Ok(Next::Call(IoCall::Delay(POLL_DELAY)))
    }

    /// The answer of an ended child, from the wrapper's control files where they exist and the
    /// child's own result where they do not.
    fn collected(
        &mut self,
        projection: JobProjection,
        files: BTreeMap<String, Vec<u8>>,
    ) -> TaskNext {
        let text = |name: &str| {
            files
                .get(name)
                .map(|bytes| String::from_utf8_lossy(bytes).trim().to_owned())
        };
        let result = (projection.canonical_execution_id)
            .and_then(|id| {
                projection
                    .executions
                    .iter()
                    .find(|run| run.execution_id == id)
            })
            .and_then(|run| run.result.as_ref());
        let exit_code = text("exit")
            .and_then(|code| code.parse().ok())
            .or_else(|| result.and_then(|result| result.exit_code));
        let tail = |name: &str, fallback: Option<&String>| {
            files
                .get(name)
                .cloned()
                .unwrap_or_else(|| fallback.cloned().unwrap_or_default().into_bytes())
        };
        let stdout = result
            .and_then(|result| result.stdout.as_ref())
            .map(|tail| tail.as_str().to_owned());
        let stderr = result
            .and_then(|result| result.stderr.as_ref())
            .map(|tail| tail.as_str().to_owned());
        let (bucket, folder) = attempt_folder(&self.run, &self.request.name).unwrap_or_default();
        let outputs: Vec<TaskOutput> = (text("manifest").unwrap_or_default().lines())
            .filter_map(|line| line.split_once(' '))
            .filter(|(kind, path)| {
                matches!(*kind, "file" | "directory")
                    && !path.is_empty()
                    && !path.contains('\0')
                    && !path.split('/').any(|part| part == "..")
            })
            .map(|(kind, path)| {
                let key = match path.strip_prefix('/') {
                    Some(absolute) => format!("{folder}control/root/{absolute}"),
                    None => format!("{folder}work/{path}"),
                };
                TaskOutput {
                    path: path.to_owned(),
                    directory: kind == "directory",
                    address: address_of(&bucket, &key, None),
                    digest: None,
                    size: 0,
                }
            })
            .collect();
        let seconds = |name: &str| text(name).and_then(|value| value.parse::<f64>().ok());
        let finished = TaskFinished {
            exit_code,
            error: None,
            retryable: false,
            cancelled: false,
            outputs,
            stdout_tail: tail("stdout", stdout.as_ref()),
            stderr_tail: tail("stderr", stderr.as_ref()),
            started: seconds("start"),
            ended: seconds("end"),
            usage: None,
        };
        let state = match exit_code {
            Some(0) => RunState::Succeeded,
            _ => RunState::Failed,
        };
        let mut task = self.record(state);
        task.child_job_id = Some(projection.canonical_job_id);
        task.exit_code = exit_code;
        task.finished_at_ms = Some(self.now_ms);
        task.started_at_ms = finished.started.map(|seconds| (seconds * 1000.0) as u64);
        task.stdout_tail = finished.stdout_tail.clone();
        task.stderr_tail = finished.stderr_tail.clone();
        task.outputs = (finished.outputs.iter())
            .map(|output| {
                let target = resolve(&self.run, &output.address).ok();
                RunFile {
                    name: output.path.clone(),
                    bucket: target.as_ref().map(|target| target.bucket.clone()),
                    key: target.map(|target| target.key),
                    node_id: Some(self.run.node_id),
                    ..RunFile::default()
                }
            })
            .collect();
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
                        let task = self.record(RunState::Queued);
                        self.change(task, TaskStep::Queued)
                    }
                }
            }
            TaskStep::Queued => {
                if !matches!(result, IoResult::Woke) {
                    changed(result)?;
                }
                self.submit()
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
    Cancel(Vec<WorkflowTaskRecord>),
}

/// Cancels child jobs of a run that have not ended: one named attempt for `CancelTask`, or all
/// of them for `StopOrphans`. Each is cancelled through its family as the run's creator.
#[derive(Debug, PartialEq)]
pub struct StopTasksOperation {
    run: WorkflowRunRecord,
    serving: (NodeId, aruna_core::structs::identity::realm::RealmId),
    name: Option<String>,
    stopped: Vec<String>,
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
            now_ms: 0,
            step: StopStep::Tasks,
        }
    }

    fn cancel_next(&mut self, mut tasks: Vec<WorkflowTaskRecord>) -> TaskNext {
        let Some(task) = tasks.pop() else {
            let stopped = std::mem::take(&mut self.stopped);
            return Ok(Next::Done(match self.name.take() {
                Some(name) => HostRpcResult::TaskCancelled { name },
                None => HostRpcResult::StopOrphans { stopped },
            }));
        };
        let Some(job_id) = task.child_job_id else {
            return self.cancel_next(tasks);
        };
        self.stopped.push(task.name.clone());
        let cancel = CancelFamilyOperation::new(
            &creator_auth(&self.run),
            job_id,
            None,
            Some(self.serving),
            self.now_ms,
        );
        self.step = StopStep::Cancel(tasks);
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
            StopStep::Cancel(tasks) => {
                if let Err(error) = result.child::<Option<()>, JobRouteError>()? {
                    warn!(%error, run = %self.run.run_id, "a child job was not cancelled");
                }
                self.cancel_next(tasks)
            }
        }
    }
}
