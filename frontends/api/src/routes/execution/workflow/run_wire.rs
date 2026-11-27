//! Request and response bodies of the workflow run routes, in the shapes the portal reads, with
//! their mapping from the run records.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::structs::execution::workflow_run::{
    RunFile, RunOrigin, RunState, StepRecord, WorkflowRunRecord, WorkflowTaskRecord,
};
use aruna_ops_work::workflows::runs::events::{LISTED_TASKS, time_text};
use aruna_ops_work::workflows::runs::reads::{LogChunk, RunSnapshot};
use aruna_workflow_contract::message::LogStream;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::{IntoParams, ToSchema};

/// One pinned input object; `source_node_id` names the node that holds it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct PinnedInputBody {
    pub bucket: String,
    pub key: String,
    pub version_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_node_id: Option<String>,
}

/// One pinned object for an input, or several.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum PinnedInputs {
    One(PinnedInputBody),
    Many(Vec<PinnedInputBody>),
}

/// The dataset that describes a run's results, when the caller asks for one.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct DatasetBody {
    pub name: String,
    pub description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub license: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public: Option<bool>,
}

/// Where a run writes its results; its task files go below `prefix` too.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct RunOutputBody {
    pub bucket: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dataset: Option<DatasetBody>,
}

/// Where a run should be placed; runs stay on the node that takes the request for now.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct PlacementBody {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub site: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ProvenanceBody {
    #[serde(default, rename = "crate", skip_serializing_if = "Option::is_none")]
    pub run_crate: Option<bool>,
}

/// One vault secret bound to a workflow run's secret name; carries only references.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct WorkflowSecretBody {
    /// `user`, `group`, `realm` or `node`.
    pub scope: String,
    /// Name of the secret stored in the vault.
    pub name: String,
    /// Field name for multi-field secrets; optional for `bearer`, `header` and `hmac`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
}

/// A run of one workflow version.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct WorkflowRunRequest {
    pub group_id: String,
    pub workflow_id: String,
    /// The version number as text, like `"2"`.
    pub version: String,
    /// The values of the workflow's form.
    #[schema(value_type = Object)]
    pub values: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inputs: Option<BTreeMap<String, PinnedInputs>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placement: Option<PlacementBody>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<RunOutputBody>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provenance: Option<ProvenanceBody>,
    /// Vault secret bindings by workflow secret name; values never enter the coordinator.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub secrets: BTreeMap<String, WorkflowSecretBody>,
}

/// A workflow run as lists and state changes show it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct WorkflowRunBody {
    pub run_id: String,
    pub group_id: String,
    pub workflow_id: String,
    pub version: String,
    /// `queued`, `planning`, `running`, `succeeded`, `failed`, `cancelling`, `cancelled` or
    /// `unknown`.
    pub state: String,
    pub node_id: Option<String>,
    pub created_at: Option<String>,
    /// When the run first turned running.
    #[serde(default)]
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub created_by: Option<String>,
    /// Why the run failed; only a real failure.
    pub error: Option<String>,
    /// A note of this node about the run, such as another node coordinating it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notice: Option<String>,
    pub lineage: Option<String>,
    pub workflow_name: Option<String>,
    /// Only on this computer; nothing about it reached the realm yet.
    pub local_only: bool,
    /// An input or the output bucket encrypts: the run reuses only its own tasks, keeps its task
    /// files in the output bucket, and its crate is read with the run, never as a group document.
    #[serde(default)]
    pub protected: bool,
    /// The caller may change the run: cancel, delete, resume or repeat it; absent when not
    /// computed for a caller.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub writable: Option<bool>,
    /// Automation that started this run, when started automatically.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub automation_id: Option<String>,
    /// Automation firing that started this run, when started automatically.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub firing_id: Option<String>,
}

/// One task of a step that lists its tasks.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct StepTaskBody {
    pub task_id: String,
    pub state: String,
    pub index: Option<u64>,
    pub cached: bool,
}

/// The states of the tasks of one step, as the node counts them.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct StepSummaryBody {
    pub step: String,
    pub state: String,
    pub total: Option<u64>,
    /// Tasks by state, and `cached` for the tasks reused from earlier runs.
    pub counts: BTreeMap<String, u64>,
    pub node_id: Option<String>,
    pub mode: Option<String>,
    /// The earliest start of a task of the step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    /// The latest end of a task, once the step has ended.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<String>,
    /// The tasks of a step with at most 25 of them; a wide scatter leaves them out.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tasks: Option<Vec<StepTaskBody>>,
}

/// One file a run delivered.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct RunFileBody {
    pub name: String,
    /// Absent while the node does not know where the file is.
    pub bucket: Option<String>,
    pub key: Option<String>,
    pub version_id: Option<String>,
    pub node_id: Option<String>,
    pub size: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct RunProvenanceBody {
    pub document_id: Option<String>,
    pub status: Option<String>,
}

/// A run with what its page shows at once: its steps, inputs, outputs and provenance.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct RunSnapshotBody {
    #[serde(flatten)]
    pub run: WorkflowRunBody,
    pub steps: Vec<StepSummaryBody>,
    #[schema(value_type = Option<Object>)]
    pub values: Option<Value>,
    pub inputs: Option<BTreeMap<String, PinnedInputs>>,
    pub outputs: Vec<RunFileBody>,
    pub output: Option<RunOutputBody>,
    pub provenance: Option<RunProvenanceBody>,
    /// The last event of the run's stream the snapshot includes.
    pub last_event_id: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct WorkflowRunPage {
    pub items: Vec<WorkflowRunBody>,
    pub next_cursor: Option<String>,
    pub total: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct WorkflowRunQuery {
    /// Only the runs of this group.
    pub group_id: Option<String>,
    /// Only the runs in this state.
    pub state: Option<String>,
    /// Only the runs of this workflow.
    pub workflow_id: Option<String>,
    /// Only the runs started by this automation.
    pub automation_id: Option<String>,
    /// Only the runs started by this automation firing.
    pub firing_id: Option<String>,
    /// The `next_cursor` of the page before.
    pub cursor: Option<String>,
    /// Runs per page, 50 when left out, at most 500.
    pub limit: Option<usize>,
}

/// One file a task read or wrote.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct TaskFileBody {
    pub name: String,
    pub bucket: Option<String>,
    pub key: Option<String>,
    pub version_id: Option<String>,
    pub node_id: Option<String>,
    pub size: Option<u64>,
    /// The digest of the content, where the node knows it.
    pub digest: Option<String>,
}

/// One task of a run, with its newest attempt.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct WorkflowTaskBody {
    pub task_id: String,
    pub step: String,
    pub state: String,
    pub attempt: u32,
    pub node_id: Option<String>,
    pub cached: bool,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub exit_code: Option<i32>,
    pub index: Option<u64>,
    pub mode: Option<String>,
    pub staged_bytes: Option<u64>,
    pub cpu_seconds: Option<f64>,
    pub max_rss_bytes: Option<u64>,
    /// What the task does and reads, as its cache key; another key means it does something else.
    pub cache_key: Option<String>,
    pub inputs: Vec<TaskFileBody>,
    pub outputs: Vec<TaskFileBody>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct WorkflowTaskPage {
    pub items: Vec<WorkflowTaskBody>,
    pub next_cursor: Option<String>,
    pub total: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct WorkflowTaskQuery {
    /// The `next_cursor` of the page before.
    pub cursor: Option<String>,
    /// Tasks per page, 50 when left out, at most 500.
    pub limit: Option<usize>,
    /// Only the tasks of this step.
    pub step: Option<String>,
    /// Only the tasks in this state.
    pub state: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct TaskLogQuery {
    /// `stdout` (the default) or `stderr`.
    pub stream: Option<String>,
    /// The byte offset to read from; 0 when left out.
    pub offset: Option<u64>,
}

/// One piece of a task's output stream.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct LogChunkBody {
    pub stream: String,
    pub offset: u64,
    pub text: String,
    pub next_offset: u64,
    /// The task ended and nothing follows `next_offset`.
    pub complete: bool,
}

/// The object version a run names as its origin.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct RunOriginBody {
    pub bucket: String,
    pub key: String,
    pub version_id: String,
}

#[derive(Clone, Debug, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct RunEventsQuery {
    /// The last sequence seen; the stream sends only what follows it. `Last-Event-ID` does the
    /// same, and the larger of both wins.
    pub after: Option<u64>,
}

/// The two runs a comparison takes.
#[derive(Clone, Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct CompareQuery {
    /// The first run, as a 26-character ULID.
    pub first: String,
    /// The second run, as a 26-character ULID.
    pub second: String,
}

/// Why a task ran, ran again or reused a cached result.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct WhyReportBody {
    pub run_id: String,
    pub task_id: String,
    /// `cached`, `reran` or `ran`.
    pub verdict: String,
    pub summary: String,
    pub cache_key: Option<String>,
    /// The task runs without network, reads every input by content and names a pinned image.
    pub deterministic: Option<bool>,
    /// One entry per difference, each with a `field` and, where known, `before` and `after`.
    #[schema(value_type = Vec<Object>)]
    pub changes: Vec<Value>,
    /// The earlier run and task of the lineage that uwe compared with.
    #[schema(value_type = Option<Object>)]
    pub earlier: Option<Value>,
    /// `uwe` for uwe's comparison, `record` for an answer from the task's record.
    pub source: String,
}

fn node_text(node: Option<aruna_core::id::NodeId>) -> Option<String> {
    node.map(|node| node.to_string())
}

impl From<&WorkflowRunRecord> for WorkflowRunBody {
    fn from(record: &WorkflowRunRecord) -> Self {
        Self {
            run_id: record.run_id.to_string(),
            group_id: record.group_id.to_string(),
            workflow_id: record.workflow_id.to_string(),
            version: record.version.to_string(),
            state: record.state.as_str().to_owned(),
            node_id: Some(record.node_id.to_string()),
            created_at: time_text(record.created_at_ms),
            started_at: record.started_at_ms.and_then(time_text),
            finished_at: record.finished_at_ms.and_then(time_text),
            created_by: Some(record.created_by.to_string()),
            error: record.error.clone(),
            notice: None,
            lineage: Some(record.lineage.to_string()),
            workflow_name: Some(record.workflow_name.clone()),
            local_only: record.local_only,
            protected: record.protected,
            writable: None,
            automation_id: record
                .automation
                .as_ref()
                .map(|origin| origin.automation_id.to_string()),
            firing_id: record
                .automation
                .as_ref()
                .map(|origin| origin.firing_id.to_string()),
        }
    }
}

impl StepSummaryBody {
    pub fn new(step: &StepRecord, tasks: Option<&Vec<WorkflowTaskRecord>>) -> Self {
        let counts = BTreeMap::from([
            (RunState::Queued.as_str().to_owned(), step.queued),
            (RunState::Running.as_str().to_owned(), step.running),
            (RunState::Succeeded.as_str().to_owned(), step.succeeded),
            (RunState::Failed.as_str().to_owned(), step.failed),
            (RunState::Cancelled.as_str().to_owned(), step.cancelled),
            ("cached".to_owned(), step.cached),
        ]);
        let listed = (step.total() <= LISTED_TASKS).then(|| {
            (tasks.into_iter().flatten())
                .map(|task| StepTaskBody {
                    task_id: task.task_id.to_string(),
                    state: task.state.as_str().to_owned(),
                    index: task.index,
                    cached: task.cached,
                })
                .collect()
        });
        let state = step.state();
        Self {
            step: step.step.clone(),
            state: state.as_str().to_owned(),
            total: Some(step.total()),
            counts,
            node_id: node_text(step.node_id),
            mode: step.mode.clone(),
            started_at: step.started_at_ms.and_then(time_text),
            finished_at: (step.finished_at_ms.filter(|_| state.is_terminal())).and_then(time_text),
            tasks: listed,
        }
    }
}

impl From<&RunFile> for RunFileBody {
    fn from(file: &RunFile) -> Self {
        Self {
            name: file.name.clone(),
            bucket: file.bucket.clone(),
            key: file.key.clone(),
            version_id: file.version_id.clone(),
            node_id: node_text(file.node_id),
            size: file.size,
        }
    }
}

impl From<&RunFile> for TaskFileBody {
    fn from(file: &RunFile) -> Self {
        Self {
            name: file.name.clone(),
            bucket: file.bucket.clone(),
            key: file.key.clone(),
            version_id: file.version_id.clone(),
            node_id: node_text(file.node_id),
            size: file.size,
            digest: file.digest.clone(),
        }
    }
}

impl From<RunSnapshot> for RunSnapshotBody {
    fn from(snapshot: RunSnapshot) -> Self {
        let record = &snapshot.record;
        let steps = (record.steps.iter())
            .map(|step| StepSummaryBody::new(step, snapshot.listed.get(&step.step)))
            .collect();
        let output = record.output.as_ref().map(|output| RunOutputBody {
            bucket: output.bucket.clone(),
            prefix: Some(output.prefix.clone()).filter(|prefix| !prefix.is_empty()),
            node_id: node_text(output.node_id),
            dataset: (output.dataset_json.as_deref())
                .and_then(|text| serde_json::from_str(text).ok()),
        });
        let provenance = record.provenance_crate.then(|| RunProvenanceBody {
            document_id: record.provenance_document_id.map(|id| id.to_string()),
            status: record.provenance_status.clone(),
        });
        Self {
            run: WorkflowRunBody {
                writable: Some(snapshot.writable),
                ..WorkflowRunBody::from(record)
            },
            steps,
            values: serde_json::from_str(&record.values_json).ok(),
            inputs: (record.inputs_json.as_deref())
                .and_then(|text| serde_json::from_str(text).ok()),
            outputs: record.outputs.iter().map(RunFileBody::from).collect(),
            output,
            provenance,
            last_event_id: record.last_event_seq,
        }
    }
}

impl From<&WorkflowTaskRecord> for WorkflowTaskBody {
    fn from(task: &WorkflowTaskRecord) -> Self {
        Self {
            task_id: task.task_id.to_string(),
            step: task.step.clone(),
            state: task.state.as_str().to_owned(),
            attempt: task.attempt,
            node_id: node_text(task.node_id),
            cached: task.cached,
            started_at: task.started_at_ms.and_then(time_text),
            finished_at: task.finished_at_ms.and_then(time_text),
            exit_code: task.exit_code,
            index: task.index,
            mode: task.mode.clone(),
            staged_bytes: task.staged_bytes,
            cpu_seconds: task.cpu_seconds,
            max_rss_bytes: task.max_rss_bytes,
            cache_key: task.cache_key.clone(),
            inputs: task.inputs.iter().map(TaskFileBody::from).collect(),
            outputs: task.outputs.iter().map(TaskFileBody::from).collect(),
        }
    }
}

impl From<LogChunk> for LogChunkBody {
    fn from(chunk: LogChunk) -> Self {
        let stream = match chunk.stream {
            LogStream::Stdout => "stdout",
            LogStream::Stderr => "stderr",
        };
        Self {
            stream: stream.to_owned(),
            offset: chunk.offset,
            text: chunk.text,
            next_offset: chunk.next_offset,
            complete: chunk.complete,
        }
    }
}

impl From<RunOrigin> for RunOriginBody {
    fn from(origin: RunOrigin) -> Self {
        Self {
            bucket: origin.bucket,
            key: origin.key,
            version_id: origin.version_id,
        }
    }
}
