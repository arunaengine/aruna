//! The calls the coordinator makes to its host node, for images, tasks, data and the run log,
//! and the answers the node gives.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ContractError;
use crate::settings::ArunaStepSettings;

/// One call of the coordinator; the node answers it with an `RpcResponse` of the same id.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "call", rename_all = "snake_case")]
pub enum HostRpcRequest {
    /// The image pinned to its digest, as the node or the named site resolves it.
    PinImage {
        image: String,
        site: Option<String>,
    },
    /// Runs one attempt of a task as a child job; answered when the attempt ended.
    StartTask(Box<StartTask>),
    /// Stops one running attempt, by its name; its `StartTask` call ends as cancelled.
    CancelTask {
        name: String,
    },
    /// Stops the child jobs of the run that are still running.
    StopOrphans {
        run_id: String,
    },
    StorageStat {
        address: String,
    },
    StorageList {
        address: String,
    },
    /// Reads at most `DATA_CHUNK_BYTES` of a file from `offset` on.
    StorageDownload {
        address: String,
        offset: u64,
    },
    /// Opens an upload of `size` bytes; the chunks follow under the returned upload id.
    StorageUploadStart {
        address: String,
        size: u64,
        digest: Option<String>,
    },
    StorageUploadChunk {
        upload_id: String,
        seq: u64,
        #[serde(with = "crate::encoding")]
        bytes: Vec<u8>,
        eof: bool,
    },
    /// Copies a finished output to where the workflow publishes it.
    DeliverOutput {
        source_ref: String,
        target_path: String,
    },
    /// The run starts; the node stores the start record of its run log.
    RunLogBegin {
        run_start_json: Value,
    },
    /// One change of the run, appended to the run log in `seq` order.
    RunLogAppend {
        seq: u64,
        change_json: Value,
        is_event: bool,
    },
}

/// One attempt of a task as the node runs it as a child job.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StartTask {
    /// uwe's canonical task specification, which the node hashes into the task key.
    pub task_key_seed: String,
    /// The step of the workflow and the task's number in the run.
    pub step_id: String,
    pub task_id: Option<u64>,
    pub attempt: u32,
    /// The unique name of the attempt, like `<run>-<task>-<attempt>`.
    pub name: String,
    pub spec_json: Value,
    /// The uwe wrapper script that runs the task in its folders.
    #[serde(with = "crate::encoding")]
    pub command_wrapper_bytes: Vec<u8>,
    pub folders: TaskFolders,
    pub image: Option<String>,
    pub resources: TaskResources,
    pub site: Option<String>,
    pub network_allowed: bool,
    pub direct_inputs: Vec<DirectInput>,
    /// Inputs the coordinator uploaded first, under the given upload address.
    pub staged_files: Vec<StagedFile>,
    pub expected_outputs: Vec<ExpectedOutput>,
    /// The environment variables whose values the node takes from the run's secrets.
    pub secret_placeholders: Vec<SecretPlaceholder>,
    pub step_settings: Option<ArunaStepSettings>,
}

/// Where the wrapper expects its folders inside the task container.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskFolders {
    pub work: String,
    pub control: String,
    pub home: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskResources {
    pub cpus: u32,
    pub memory_bytes: Option<u64>,
    pub disk_bytes: Option<u64>,
    pub time_seconds: Option<u64>,
    pub gpus: u32,
}

/// A task input the node gives the child job by its address, so no bytes pass the coordinator.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectInput {
    /// Where the task sees it, relative to the work folder or absolute.
    pub path: String,
    pub address: String,
    /// `sha256:<hex>` of the content.
    pub digest: String,
    pub size: u64,
    pub executable: bool,
    pub aruna_id: Option<String>,
    pub endpoints: Vec<String>,
}

/// An input that only the coordinator held, uploaded before the task starts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagedFile {
    pub path: String,
    /// The upload address that holds its bytes; `None` for a directory to create.
    pub address: Option<String>,
    pub directory: bool,
    pub executable: bool,
    pub size: u64,
}

/// A pattern the wrapper collects outputs by, relative to the work folder.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpectedOutput {
    pub pattern: String,
}

/// An environment variable whose value stands in for a secret of the run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretPlaceholder {
    pub name: String,
    pub placeholder: String,
}

/// The answer to one call; a refused call answers `Failed`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "answer", rename_all = "snake_case")]
pub enum HostRpcResult {
    PinImage {
        pinned: Option<String>,
    },
    TaskFinished(Box<TaskFinished>),
    TaskCancelled {
        name: String,
    },
    StopOrphans {
        stopped: Vec<String>,
    },
    StorageStat {
        entry: Option<StorageEntry>,
    },
    StorageList {
        entries: Vec<ListEntry>,
    },
    StorageChunk {
        #[serde(with = "crate::encoding")]
        bytes: Vec<u8>,
        eof: bool,
        version: Option<String>,
    },
    UploadStarted {
        upload_id: String,
    },
    /// The chunk is stored; the last chunk carries the receipt of the upload.
    UploadAck {
        seq: u64,
        receipt: Option<UploadReceipt>,
    },
    Delivered {
        location: String,
    },
    RunLogAck {
        seq: u64,
    },
    Failed(ContractError),
}

/// How an attempt ended, with what the wrapper left in its control folder.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TaskFinished {
    pub exit_code: Option<i32>,
    /// Why the node could not run the attempt; another attempt may work when `retryable`.
    pub error: Option<String>,
    #[serde(default)]
    pub retryable: bool,
    pub cancelled: bool,
    /// The outputs the wrapper collected, each readable at its address.
    pub outputs: Vec<TaskOutput>,
    #[serde(with = "crate::encoding")]
    pub stdout_tail: Vec<u8>,
    #[serde(with = "crate::encoding")]
    pub stderr_tail: Vec<u8>,
    /// Seconds since the Unix epoch, as the wrapper wrote them.
    pub started: Option<f64>,
    pub ended: Option<f64>,
    pub usage: Option<TaskUsage>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskOutput {
    /// The path as the task named it, relative to its work folder or absolute.
    pub path: String,
    pub directory: bool,
    pub address: String,
    pub digest: Option<String>,
    pub size: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskUsage {
    pub cpu_usec: Option<u64>,
    pub memory_peak: Option<u64>,
    pub read_bytes: Option<u64>,
    pub write_bytes: Option<u64>,
}

/// What the node knows of a file or folder at an address.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageEntry {
    pub directory: bool,
    pub size: u64,
    /// Milliseconds since the Unix epoch.
    pub modified_ms: Option<i64>,
    pub version: Option<String>,
    /// The SHA-256 of the content as 64 lowercase hexadecimal digits, when the node verified it.
    pub sha256: Option<String>,
    pub aruna_id: Option<String>,
    pub endpoints: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListEntry {
    pub name: String,
    pub directory: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadReceipt {
    pub address: String,
    pub size: u64,
    pub version: Option<String>,
    pub sha256: Option<String>,
}
