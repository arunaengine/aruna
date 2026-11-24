//! The messages of both directions of the channel: the greeting, preflight requests, the run
//! start, call answers, live events and the end of a run.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::ContractError;
use crate::host::{HostRpcRequest, HostRpcResult};
use crate::preflight::WorkflowSource;
use crate::preflight::{PreflightKind, PreflightResult, RunLogPage, WorkflowLanguage};
use crate::settings::ArunaStepSettings;

/// What a coordinator attempt is started for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoordinatorMode {
    /// Answers preflight requests and runs no task.
    Preflight,
    /// Runs one workflow run.
    Run,
    /// Answers report requests over recorded run log pages.
    Report,
}

/// From the node to the coordinator.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HostToCoordinator {
    /// The first frame; the coordinator answers `HelloAck` or ends the channel.
    Hello {
        protocol_version: u32,
        run_id: String,
        /// Ties the attempt to the claim of the run job, without the token itself.
        claim_token_digest: String,
        mode: CoordinatorMode,
    },
    PreflightRequest {
        request_id: u64,
        kind: PreflightKind,
    },
    RunStart(RunStartPayload),
    RpcResponse {
        call_id: u64,
        result: HostRpcResult,
    },
    /// Stops the run: running tasks are stopped and the run ends as stopped.
    Cancel {
        reason: String,
    },
    /// Ends the channel after the work in flight.
    Shutdown,
}

/// From the coordinator to the node.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CoordinatorToHost {
    HelloAck {
        protocol_version: u32,
        /// The uwe-rs revision the coordinator was built from.
        uwe_revision: String,
        capabilities: Vec<String>,
    },
    PreflightResponse {
        request_id: u64,
        result: Result<PreflightResult, ContractError>,
    },
    RpcRequest {
        call_id: u64,
        request: HostRpcRequest,
    },
    LiveEvent {
        seq: u64,
        event: LiveWorkflowEvent,
    },
    RunFinished(RunFinishedPayload),
}

/// Everything a run needs; the node gives the run id in `Hello`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RunStartPayload {
    pub source: WorkflowSource,
    pub entrypoint: String,
    pub language: Option<WorkflowLanguage>,
    pub inputs: Value,
    pub params: Map<String, Value>,
    /// Images as the workflow names them, pinned to digests at import.
    pub pinned_images: BTreeMap<String, String>,
    pub cache_namespace: Option<String>,
    /// The earlier run of the lineage that this run resumes; its log is in `prior_log_pages`.
    pub resume_lineage: Option<String>,
    /// Names of the run's secrets; the coordinator never sees their values.
    pub secret_names: Vec<String>,
    pub network_isolated: bool,
    pub max_pending: Option<u32>,
    pub prior_log_pages: Vec<RunLogPage>,
    /// The `aruna` settings of composed workflow steps, by step id.
    pub step_settings: BTreeMap<String, ArunaStepSettings>,
}

/// How a run ended.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RunFinishedPayload {
    pub success: bool,
    pub error: Option<String>,
    /// The run was cancelled, which is no failure of its own.
    pub stopped: bool,
    /// The run's result values and task counts.
    pub final_summary_json: Value,
}

/// One live event of a run, for the node's event stream.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum LiveWorkflowEvent {
    RunStarted {
        run_id: String,
        time_ms: i64,
    },
    TaskChanged(TaskChange),
    /// New output lines of a running attempt.
    TaskLog {
        task_id: u64,
        attempt: u32,
        stream: LogStream,
        chunk: String,
        time_ms: i64,
    },
    TaskUsage {
        task_id: u64,
        attempt: u32,
        cpu_percent: Option<f64>,
        memory_bytes: Option<u64>,
        time_ms: i64,
    },
    /// The workflow found that later tasks depend on the outputs of a task.
    TaskMarked {
        task_id: u64,
        kind: String,
        reason: String,
    },
    /// How many tasks are in each state now.
    Progress(ProgressCounts),
    /// A line the workflow printed, with secret values hidden.
    Printed {
        text: String,
    },
    RunEnded {
        run_id: String,
        time_ms: i64,
        error: Option<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TaskChange {
    pub task_id: u64,
    pub name: String,
    pub step: String,
    pub state: TaskState,
    pub attempt: Option<u32>,
    pub time_ms: i64,
    pub started_ms: Option<i64>,
    pub ended_ms: Option<i64>,
    pub cached: bool,
    pub exit_code: Option<i32>,
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Requested,
    Started,
    Retried,
    Finished,
    Reused,
    Failed,
    Stopped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogStream {
    Stdout,
    Stderr,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgressCounts {
    pub requested: u64,
    pub running: u64,
    pub finished: u64,
    #[serde(default)]
    pub cached: u64,
    pub failed: u64,
    #[serde(default)]
    pub stopped: u64,
}
