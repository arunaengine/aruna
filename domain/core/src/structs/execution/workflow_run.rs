//! Records of workflow runs on one node: the run, its tasks, its event stream rows, its paged run
//! log, its idempotency index and the task cache, with their storage keys and codecs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_workflow_contract::preflight::RunLogPage;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::errors::ConversionError;
use crate::id::NodeId;
use crate::secrets::SecretName;
use crate::structs::execution::job::identity::JobId;
use crate::structs::execution::job::input::{CapturedInput, InputSelection};
use crate::structs::identity::auth::PathRestriction;
use crate::structs::identity::realm::RealmId;
use crate::types::{GroupId, Key};
use crate::user::id::UserId;

/// One remote pin resolved and granted at workflow admission, keyed by the requested pin.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrecapturedInput {
    pub pin_node_id: NodeId,
    pub pin_bucket: String,
    pub pin_key: String,
    pub pin_version_id: Ulid,
    /// The resolved target selection after following links on the source holder.
    pub resolved: InputSelection,
    pub captured: CapturedInput,
}

/// Constant-size authoritative run verdict for remote vault secret verification.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowRunAuth {
    pub realm_id: RealmId,
    pub group_id: GroupId,
    pub workflow_id: Ulid,
    pub created_by: UserId,
    pub local_only: bool,
    pub bindings_match: bool,
}

/// Which replicated payload a bounded chunk belongs to.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChunkKind {
    Source,
    Tool(String, String),
    Page(u32),
    Run,
    Workflow,
    Version,
}

/// One bounded piece of a large replicated workflow catalog, run, tool, or log page record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicatedChunk {
    pub kind: ChunkKind,
    pub index: u32,
    pub total: u32,
    pub data: Vec<u8>,
}

/// Immutable catalog records copied alongside a run so a surviving holder can resume it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicatedWorkflow {
    pub workflow: Vec<u8>,
    pub version: Vec<u8>,
    pub source: Option<Vec<u8>>,
    /// Group tool definitions required to lower a composed `.uwe.yaml` workflow after failover.
    #[serde(default)]
    pub tools: Vec<Vec<u8>>,
    /// Bounded chunks of a large payload; assembled when `index + 1 == total`.
    #[serde(default)]
    pub chunks: Vec<ReplicatedChunk>,
    /// True while earlier metadata frames are still arriving; the run commits on the final frame.
    #[serde(default)]
    pub more: bool,
    /// True when the outer `StoreRunLog.run` is an identity stub and the full run is chunked.
    #[serde(default)]
    pub stub_run: bool,
    /// Sender-chosen transfer identifier scoping staged chunks and catalog rows to one transfer.
    #[serde(default)]
    pub transfer_id: u64,
}

/// Which secret store a workflow secret binding reads from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowSecretScope {
    User,
    Group,
    Realm,
    Node,
}

/// One vault secret bound to a workflow run's secret name; carries only references, never values.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowSecretBinding {
    pub scope: WorkflowSecretScope,
    pub name: SecretName,
    pub field: Option<String>,
}

/// What the run job of a workflow run carries; the run record holds everything else.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowRunSpec {
    pub run_id: Ulid,
    pub group_id: GroupId,
    pub created_by: UserId,
    pub realm_id: RealmId,
}

/// The state of a run, or of one of its tasks or steps, as the portal names it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    #[default]
    Queued,
    Planning,
    Running,
    Succeeded,
    Failed,
    Cancelling,
    Cancelled,
    Unknown,
}

impl RunState {
    pub const ALL: [RunState; 8] = [
        Self::Queued,
        Self::Planning,
        Self::Running,
        Self::Succeeded,
        Self::Failed,
        Self::Cancelling,
        Self::Cancelled,
        Self::Unknown,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Planning => "planning",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelling => "cancelling",
            Self::Cancelled => "cancelled",
            Self::Unknown => "unknown",
        }
    }

    pub fn parse(text: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|state| state.as_str() == text)
    }

    /// A run in this state changes no more.
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }

    /// A run in this state may still change, so it cannot be deleted.
    pub const fn is_active(self) -> bool {
        matches!(
            self,
            Self::Queued | Self::Planning | Self::Running | Self::Cancelling
        )
    }
}

/// Where a run writes its results; its task files go below `prefix` too.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunOutput {
    pub bucket: String,
    pub prefix: String,
    pub node_id: Option<NodeId>,
    /// The dataset the portal asked for, as JSON text.
    pub dataset_json: Option<String>,
}

/// One file a run or a task read or wrote.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunFile {
    pub name: String,
    pub bucket: Option<String>,
    pub key: Option<String>,
    pub version_id: Option<String>,
    pub node_id: Option<NodeId>,
    pub size: Option<u64>,
    pub digest: Option<String>,
}

/// Which runs may reuse a task result: the run itself, its group, or the whole realm.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheScope {
    /// Only the run that made the result; no other run reuses it.
    Run,
    #[default]
    Group,
    /// Off unless a run asks for it; a hit from another group never names its producer.
    Realm,
}

impl CacheScope {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Run => "run",
            Self::Group => "group",
            Self::Realm => "realm",
        }
    }
}

/// One reusable result of a task that ended with exit code 0, under a scope's namespace.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowCacheRecord {
    pub task_key: String,
    /// `run:<run_id>`, `group:<group_id>` or `realm`.
    pub scope_namespace: String,
    pub exit_code: i32,
    /// The outputs by their task path, each pinned to the version the task wrote.
    pub outputs: Vec<RunFile>,
    pub stdout_tail: Vec<u8>,
    pub stderr_tail: Vec<u8>,
    pub created_at_ms: u64,
    pub producer_group_id: GroupId,
    pub producer_run_id: Ulid,
    /// No network, every input by content and a pinned image.
    pub deterministic: bool,
    pub has_secrets: bool,
}

/// One object version a run names as its origin.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunOrigin {
    pub bucket: String,
    pub key: String,
    pub version_id: String,
}

/// One versioned file captured from a directory-valued input parameter at admission.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectoryPin {
    pub bucket: String,
    pub key: String,
    pub version_id: Ulid,
    pub source_node_id: Option<NodeId>,
}

/// How many tasks of one step are in each state, kept with the run so a read needs no scan.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepRecord {
    pub step: String,
    pub queued: u64,
    pub running: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub cancelled: u64,
    pub cached: u64,
    pub node_id: Option<NodeId>,
    pub mode: Option<String>,
    /// The earliest start of a task of the step.
    #[serde(default)]
    pub started_at_ms: Option<u64>,
    /// The latest end of a task of the step.
    #[serde(default)]
    pub finished_at_ms: Option<u64>,
}

impl StepRecord {
    pub fn total(&self) -> u64 {
        self.queued + self.running + self.succeeded + self.failed + self.cancelled
    }

    /// Moves one task out of `old` and into `new`; `None` leaves a side out.
    pub fn count(&mut self, old: Option<RunState>, new: Option<RunState>) {
        if let Some(slot) = old.and_then(|state| self.slot(state)) {
            *slot = slot.saturating_sub(1);
        }
        if let Some(slot) = new.and_then(|state| self.slot(state)) {
            *slot += 1;
        }
    }

    fn slot(&mut self, state: RunState) -> Option<&mut u64> {
        match state {
            RunState::Queued | RunState::Planning => Some(&mut self.queued),
            RunState::Running | RunState::Cancelling => Some(&mut self.running),
            RunState::Succeeded => Some(&mut self.succeeded),
            RunState::Failed => Some(&mut self.failed),
            RunState::Cancelled => Some(&mut self.cancelled),
            RunState::Unknown => None,
        }
    }

    /// The state of the step as a whole: running while any task waits or runs.
    pub fn state(&self) -> RunState {
        match self {
            step if step.running > 0 => RunState::Running,
            step if step.queued > 0 && step.total() > step.queued => RunState::Running,
            step if step.queued > 0 => RunState::Queued,
            step if step.failed > 0 => RunState::Failed,
            step if step.cancelled > 0 => RunState::Cancelled,
            step if step.succeeded > 0 => RunState::Succeeded,
            _ => RunState::Unknown,
        }
    }
}

/// One workflow run of a group on this node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowRunRecord {
    pub run_id: Ulid,
    /// The realm the run was admitted in, which its permission checks name.
    pub realm_id: RealmId,
    pub group_id: GroupId,
    pub workflow_id: Ulid,
    pub workflow_name: String,
    pub version: u32,
    /// The first run of the lineage this run belongs to; a new lineage starts with the run.
    pub lineage: Ulid,
    pub state: RunState,
    pub node_id: NodeId,
    pub created_by: UserId,
    pub created_at_ms: u64,
    pub finished_at_ms: Option<u64>,
    pub error: Option<String>,
    pub values_json: String,
    /// The pinned inputs of the request, as JSON text.
    pub inputs_json: Option<String>,
    pub output: Option<RunOutput>,
    /// The files the run delivered to its output.
    pub outputs: Vec<RunFile>,
    pub provenance_crate: bool,
    pub provenance_document_id: Option<Ulid>,
    pub provenance_status: Option<String>,
    /// The sequence of the newest event row; event sequences start at 1.
    pub last_event_seq: u64,
    pub local_only: bool,
    pub origin: Option<RunOrigin>,
    pub parent_job_id: Option<JobId>,
    pub claim_token_digest: Option<String>,
    /// Coordinator attempts started so far.
    pub attempts: u32,
    /// The task counts of each step, in the order the steps first appeared.
    pub steps: Vec<StepRecord>,
    /// The run this run resumes; finished tasks of its lineage are not run again.
    pub resumes_run_id: Option<Ulid>,
    /// The run this run repeats from the same sources, values and images.
    pub repeats_run_id: Option<Ulid>,
    pub cache_scope: CacheScope,
    /// The site the request placed the run's tasks at; a task names no other site.
    #[serde(default)]
    pub placement_site: Option<String>,
    /// An input or the output bucket encrypts: the run reuses no other run's tasks, keeps its task
    /// files in the output bucket and publishes no group document of its crate.
    #[serde(default)]
    pub protected: bool,
    /// When the run first turned running.
    #[serde(default)]
    pub started_at_ms: Option<u64>,
    /// Vault secret bindings of the run, by workflow secret name; carries only references.
    #[serde(default)]
    pub secrets: BTreeMap<String, WorkflowSecretBinding>,
    /// Caller path restrictions captured at admission and enforced throughout execution.
    #[serde(default)]
    pub path_restrictions: Option<Vec<PathRestriction>>,
    /// Remote pinned inputs resolved and granted at admission for reuse by child tasks.
    #[serde(default)]
    pub captured_remote: Vec<PrecapturedInput>,
    /// Versioned file entries captured from directory-valued parameters for cross-node resume.
    #[serde(default)]
    pub directory_pins: Vec<DirectoryPin>,
    /// Resolved `(tool_id, version)` pairs a composed workflow lowered against for this run.
    #[serde(default)]
    pub used_tools: Vec<(String, String)>,
}

/// One task of a run, with the state of its newest attempt.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkflowTaskRecord {
    pub run_id: Ulid,
    pub task_id: u64,
    /// The key of uwe's canonical task specification; another key means another task.
    pub task_key: String,
    pub step: String,
    /// The unique name of the newest attempt, like `<run>-<task>-<attempt>`.
    pub name: String,
    pub state: RunState,
    pub attempt: u32,
    pub index: Option<u64>,
    pub child_job_id: Option<JobId>,
    pub node_id: Option<NodeId>,
    pub mode: Option<String>,
    pub cached: bool,
    pub cache_key: Option<String>,
    /// The task runs without network, reads every input by content and names a pinned image.
    pub deterministic: Option<bool>,
    pub staged_bytes: Option<u64>,
    pub cpu_seconds: Option<f64>,
    pub max_rss_bytes: Option<u64>,
    pub started_at_ms: Option<u64>,
    pub finished_at_ms: Option<u64>,
    pub exit_code: Option<i32>,
    pub error: Option<String>,
    pub inputs: Vec<RunFile>,
    pub outputs: Vec<RunFile>,
    pub stdout_tail: Vec<u8>,
    pub stderr_tail: Vec<u8>,
}

/// One row of a run's event stream, replayed after a sequence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunEventRecord {
    pub seq: u64,
    /// `run`, `state`, `step` or `task`.
    pub event_type: String,
    pub payload_json: String,
}

/// The run an idempotency key of a caller started, and the digest of what it asked for.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunKeyRecord {
    pub run_id: Ulid,
    pub request_digest: [u8; 32],
}

macro_rules! postcard_codec {
    ($($record:ty),*) => {$(
        impl $record {
            pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
                Ok(postcard::to_allocvec(self)?)
            }

            pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
                Ok(postcard::from_bytes(bytes)?)
            }
        }
    )*};
}

postcard_codec!(
    WorkflowRunRecord,
    WorkflowTaskRecord,
    RunEventRecord,
    RunKeyRecord,
    LogHead,
    WorkflowCacheRecord
);

/// A run log page as JSON, since its entries carry uwe's changes as free JSON.
pub fn encode_page(page: &RunLogPage) -> Result<Vec<u8>, ConversionError> {
    Ok(serde_json::to_vec(page)?)
}

pub fn decode_page(bytes: &[u8]) -> Result<RunLogPage, ConversionError> {
    Ok(serde_json::from_slice(bytes)?)
}

/// The row of a run: its id.
pub fn run_key(run_id: Ulid) -> Key {
    Key::from(run_id.to_bytes().to_vec())
}

/// The rows of a run's tasks, events or log pages all start with the run id.
pub fn run_prefix(run_id: Ulid) -> Key {
    run_key(run_id)
}

/// The row of a task: the run id, then the task number big-endian.
pub fn task_key(run_id: Ulid, task_id: u64) -> Key {
    numbered(run_id, &task_id.to_be_bytes())
}

/// The row of an event: the run id, then its sequence big-endian, so rows list in order.
pub fn event_key(run_id: Ulid, seq: u64) -> Key {
    numbered(run_id, &seq.to_be_bytes())
}

/// Where the run log of a run stands: how many pages it has and the newest change it holds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogHead {
    pub pages: u32,
    pub entries: u32,
    pub last_seq: Option<u64>,
    pub begun: bool,
}

/// The row of a run log's head; it sorts after every page of the run.
pub fn head_key(run_id: Ulid) -> Key {
    page_key(run_id, u32::MAX)
}

/// The row of a run log page: the run id, then the page number big-endian.
pub fn page_key(run_id: Ulid, page: u32) -> Key {
    numbered(run_id, &page.to_be_bytes())
}

/// Prefix covering all replica staging rows of `run_id` written by `peer`.
pub fn stage_prefix(run_id: Ulid, peer: NodeId) -> Key {
    let mut key = Vec::with_capacity(49);
    key.extend_from_slice(&run_id.to_bytes());
    key.push(b's');
    key.extend_from_slice(peer.as_bytes());
    Key::from(key)
}

/// Prefix covering all replica staging rows of one `(run_id, peer, transfer)` transfer.
pub fn transfer_prefix(run_id: Ulid, peer: NodeId, transfer: u64) -> Key {
    let mut key = Vec::with_capacity(57);
    key.extend_from_slice(&run_id.to_bytes());
    key.push(b's');
    key.extend_from_slice(peer.as_bytes());
    key.extend_from_slice(&transfer.to_be_bytes());
    Key::from(key)
}

/// Temporary row staging one chunk under `transfer_prefix(run_id, peer, transfer)`.
pub fn chunk_key(run_id: Ulid, peer: NodeId, transfer: u64, target: [u8; 32], index: u32) -> Key {
    let mut key = Vec::with_capacity(94);
    key.extend_from_slice(&run_id.to_bytes());
    key.push(b's');
    key.extend_from_slice(peer.as_bytes());
    key.extend_from_slice(&transfer.to_be_bytes());
    key.push(b'c');
    key.extend_from_slice(&target);
    key.extend_from_slice(&index.to_be_bytes());
    Key::from(key)
}

/// One page of exact-version child files stored for a remote directory output under `run_prefix`.
pub fn dir_page_key(run_id: Ulid, task_id: u64, attempt: u32, output_index: u32, page: u32) -> Key {
    let mut key = Vec::with_capacity(37);
    key.extend_from_slice(&run_id.to_bytes());
    key.push(b'd');
    key.extend_from_slice(&task_id.to_be_bytes());
    key.extend_from_slice(&attempt.to_be_bytes());
    key.extend_from_slice(&output_index.to_be_bytes());
    key.extend_from_slice(&page.to_be_bytes());
    Key::from(key)
}

/// The row of an idempotency key: the group, the caller, then the key with its length first.
pub fn run_key_row(group_id: GroupId, created_by: UserId, key: &str) -> Key {
    let mut row = group_id.to_bytes().to_vec();
    row.extend_from_slice(&created_by.to_bytes());
    row.extend_from_slice(&(key.len() as u32).to_be_bytes());
    row.extend_from_slice(key.as_bytes());
    Key::from(row)
}

/// The row of a cached task result: the namespace with its length first, then the task key.
pub fn cache_row(namespace: &str, task_key: &str) -> Key {
    let mut row = (namespace.len() as u32).to_be_bytes().to_vec();
    row.extend_from_slice(namespace.as_bytes());
    row.extend_from_slice(task_key.as_bytes());
    Key::from(row)
}

fn numbered(run_id: Ulid, number: &[u8]) -> Key {
    let mut key = run_id.to_bytes().to_vec();
    key.extend_from_slice(number);
    Key::from(key)
}

/// The task number a sequence-free name maps to: a stable hash with the high bit set, so it never
/// meets a number uwe gives.
pub fn named_task(name: &str) -> u64 {
    let hash = blake3::hash(name.as_bytes());
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&hash.as_bytes()[..8]);
    u64::from_be_bytes(bytes) | (1 << 63)
}

#[cfg(test)]
mod tests {
    use aruna_workflow_contract::preflight::RunLogEntry;

    use super::*;

    #[test]
    fn orders_event_keys() {
        let run = Ulid::from_bytes([1; 16]);
        assert!(event_key(run, 2) < event_key(run, 10));
        assert!(event_key(run, 7).starts_with(run_prefix(run)));
        let other = Ulid::from_bytes([2; 16]);
        assert!(!task_key(other, 1).starts_with(run_prefix(run)));
    }

    #[test]
    fn separates_key_rows() {
        let group = Ulid::from_bytes([3; 16]);
        let user = UserId::default();
        assert_ne!(
            run_key_row(group, user, "ab"),
            run_key_row(group, user, "a")
        );
        assert!(named_task("run-1-1") >= 1 << 63);
        assert_eq!(named_task("x"), named_task("x"));
        assert_ne!(cache_row("group:a", "bc"), cache_row("group:ab", "c"));
        assert_eq!(cache_row("realm", "k"), cache_row("realm", "k"));
    }

    #[test]
    fn round_trips_pages() {
        let page = RunLogPage {
            entries: vec![RunLogEntry::Change {
                seq: 4,
                change_json: serde_json::json!({"task": 1}),
            }],
        };
        let bytes = encode_page(&page).unwrap();
        assert_eq!(decode_page(&bytes).unwrap(), page);
        for state in RunState::ALL {
            assert_eq!(RunState::parse(state.as_str()), Some(state));
        }
        assert!(RunState::Cancelling.is_active() && !RunState::Cancelling.is_terminal());
    }

    #[test]
    fn counts_step_states() {
        let mut step = StepRecord::default();
        step.count(None, Some(RunState::Queued));
        step.count(None, Some(RunState::Queued));
        assert_eq!(step.state(), RunState::Queued);
        step.count(Some(RunState::Queued), Some(RunState::Succeeded));
        assert_eq!(step.state(), RunState::Running);
        step.count(Some(RunState::Queued), Some(RunState::Failed));
        assert_eq!((step.total(), step.state()), (2, RunState::Failed));
    }
}
