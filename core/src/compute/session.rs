//! Defines interactive session states, caps and the protocol spoken to the session helper.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::stream::BackendStream;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::time::Duration;
use thiserror::Error;

/// Cells one session may hold queued before submits are refused.
pub const MAX_QUEUED_CELLS: usize = 64;
/// Submits one session accepts inside `SUBMIT_WINDOW`.
pub const MAX_SUBMITS: usize = 30;
/// Window the submit count is measured over.
pub const SUBMIT_WINDOW: Duration = Duration::from_secs(10);
/// Bytes of code one cell may carry.
pub const MAX_CELL_BYTES: usize = 256 * 1024;
/// Characters a cell id may have.
pub const MAX_ID_LEN: usize = 64;
/// Bytes one scratch read may return.
pub const MAX_SCRATCH_BYTES: u64 = 8 * 1024 * 1024;
/// Cells one session reports in its state.
pub const MAX_TRACKED_CELLS: usize = 512;
/// Staged inputs one session records for its report.
pub const MAX_TRACKED_INPUTS: usize = 1024;
/// Objects one session's touched set records for its report.
pub const MAX_TOUCHED_OBJECTS: usize = 1024;
/// Events one session keeps for a reconnecting client.
pub const MAX_RING_EVENTS: usize = 4096;
/// Bytes one session keeps for a reconnecting client.
pub const MAX_RING_BYTES: usize = 4 * 1024 * 1024;
/// Outputs one cell may emit before the rest are dropped.
pub const MAX_CELL_OUTPUTS: usize = 512;
/// Output bytes one cell may emit before the rest are dropped.
pub const MAX_OUTPUT_BYTES: usize = 1024 * 1024;
/// What the client sees once a cell hit either output cap.
pub const TRUNCATED_NOTICE: &str = "[output truncated by the node]";

/// What the session is doing right now.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionPhase {
    Starting,
    Ready,
    Busy,
    Ended,
}

impl SessionPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            SessionPhase::Starting => "starting",
            SessionPhase::Ready => "ready",
            SessionPhase::Busy => "busy",
            SessionPhase::Ended => "ended",
        }
    }
}

/// Why a session stopped. A node restart is not one: the node re-adopts the
/// running container and opens a new session for it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EndReason {
    Ended,
    Idle,
    Walltime,
    Cancelled,
    KernelExit,
}

impl EndReason {
    pub fn as_str(self) -> &'static str {
        match self {
            EndReason::Ended => "ended",
            EndReason::Idle => "idle",
            EndReason::Walltime => "walltime",
            EndReason::Cancelled => "cancelled",
            EndReason::KernelExit => "kernel_exit",
        }
    }
}

/// Where one cell stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CellPhase {
    Queued,
    Running,
    Done,
    Error,
    Interrupted,
}

impl CellPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            CellPhase::Queued => "queued",
            CellPhase::Running => "running",
            CellPhase::Done => "done",
            CellPhase::Error => "error",
            CellPhase::Interrupted => "interrupted",
        }
    }

    pub fn from_wire(value: &str) -> Option<Self> {
        match value {
            "queued" => Some(CellPhase::Queued),
            "running" => Some(CellPhase::Running),
            "done" => Some(CellPhase::Done),
            "error" => Some(CellPhase::Error),
            "interrupted" => Some(CellPhase::Interrupted),
            _ => None,
        }
    }

    pub fn is_open(self) -> bool {
        matches!(self, CellPhase::Queued | CellPhase::Running)
    }
}

/// Event types the stream carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventKind {
    /// The whole state object without its cells, re-sent whenever the state or
    /// the idle deadline moved.
    Session,
    Cell,
    Output,
    Kernel,
    Credential,
    Ended,
}

impl EventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            EventKind::Session => "session",
            EventKind::Cell => "cell",
            EventKind::Output => "output",
            EventKind::Kernel => "kernel",
            EventKind::Credential => "credential",
            EventKind::Ended => "ended",
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SessionError {
    #[error("the session is still starting")]
    Starting,
    #[error("the session has ended")]
    Ended,
    #[error("cell {0} is already queued or running")]
    CellBusy(String),
    #[error("too many cells queued or submitted")]
    TooMany,
    #[error("a cell id is 1 to 64 characters of A-Z, a-z, 0-9, _ and -")]
    CellId,
    #[error("cell code is larger than {MAX_CELL_BYTES} bytes")]
    CodeTooLarge,
    #[error("a scratch path is relative to the working directory and carries no `..`")]
    Path,
    #[error("the session helper did not answer")]
    NoReply,
    #[error("the session helper refused: {0}")]
    Helper(String),
}

/// One object staged into the workspace bucket after the session started.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StagedInput {
    pub dest_key: String,
    pub bytes: u64,
    pub blake3: String,
    pub source_node_id: String,
    pub version_id: String,
}

/// One object a background copy job is still bringing into the workspace
/// bucket. The report resolves it against the job once the session ends.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PendingInput {
    pub dest_key: String,
    pub job_id: String,
    pub source_node_id: String,
}

/// One object the session's own credential read or wrote, as the S3 plane
/// attributed it to this job.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct TouchedObject {
    pub bucket: String,
    pub key: String,
    /// `read` or `write`.
    pub operation: String,
}

/// One cell as the client sees it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct CellSnapshot {
    pub cell_id: String,
    pub state: CellPhase,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at_ms: Option<u64>,
}

/// The whole session as the client sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionSnapshot {
    pub job_id: String,
    pub state: SessionPhase,
    pub runtime: String,
    pub workspace_bucket: String,
    pub executor_node_id: String,
    pub started_at_ms: u64,
    pub idle_after_ms: u64,
    pub idle_deadline_ms: u64,
    pub credential_expires_ms: u64,
    pub last_event_id: u64,
    pub cells: Vec<CellSnapshot>,
    pub ended: Option<EndReason>,
}

/// One stream frame. `data` is already serialized so the ring can measure it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionEvent {
    pub id: u64,
    pub kind: EventKind,
    pub data: String,
}

/// One request the node sends to the helper.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum HelperRequest {
    Execute {
        id: u64,
        cell_id: String,
        code: String,
    },
    Interrupt {
        id: u64,
    },
    Status {
        id: u64,
    },
    List {
        id: u64,
        path: String,
    },
    Read {
        id: u64,
        path: String,
        offset: u64,
        limit: u64,
    },
}

impl HelperRequest {
    pub fn id(&self) -> u64 {
        match self {
            HelperRequest::Execute { id, .. }
            | HelperRequest::Interrupt { id }
            | HelperRequest::Status { id }
            | HelperRequest::List { id, .. }
            | HelperRequest::Read { id, .. } => *id,
        }
    }
}

/// One event the helper sends back. A reply answers `list`, `read` and
/// `status` by request id.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum HelperEvent {
    Output {
        cell_id: String,
        output: Value,
    },
    Cell {
        cell_id: String,
        state: String,
        #[serde(default)]
        execution_count: Option<u32>,
    },
    Kernel {
        state: String,
    },
    Reply {
        id: u64,
        #[serde(flatten)]
        body: Map<String, Value>,
    },
}

/// One request to the live session of a job on this node; the job id is the physical one.
#[derive(Debug, PartialEq)]
pub struct SessionEffect {
    pub job_id: String,
    pub request: SessionRequest,
}

/// What a request asks of the live session.
#[derive(Debug, PartialEq)]
pub enum SessionRequest {
    Snapshot,
    /// Frames after `after` and the live tail; `None` replays everything the ring still holds.
    Subscribe {
        after: Option<u64>,
    },
    SubmitCell {
        cell_id: String,
        code: String,
    },
    Interrupt,
    End(EndReason),
    RecordInput(StagedInput),
    RecordPending(PendingInput),
    RecordTouched(TouchedObject),
    /// Moves the idle deadline.
    Touch,
    CellOutputs {
        cell_id: String,
        after: u64,
    },
    ListScratch {
        path: String,
    },
    ReadScratch {
        path: String,
        limit: u64,
    },
}

/// The live session's answer to one request.
#[derive(Debug, PartialEq)]
pub enum SessionAnswer {
    /// No session of that job runs on this node.
    Missing,
    Done,
    Snapshot(Box<SessionSnapshot>),
    /// Replayed frames, then the live frames until the session closes.
    Stream {
        backlog: Vec<SessionEvent>,
        live: BackendStream<SessionEvent>,
    },
    /// The ring no longer reaches the requested resume point.
    Gap,
    Position(usize),
    Outputs {
        last_event_id: u64,
        outputs: Vec<Value>,
    },
    Body(Map<String, Value>),
    Refused(SessionError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_carries_op() {
        let line = serde_json::to_string(&HelperRequest::Execute {
            id: 7,
            cell_id: "c1".to_string(),
            code: "print(1)".to_string(),
        })
        .expect("request serializes");
        let parsed: Value = serde_json::from_str(&line).expect("line parses");
        assert_eq!(parsed["op"], "execute");
        assert_eq!(parsed["id"], 7);
        assert_eq!(parsed["cell_id"], "c1");
    }

    #[test]
    fn reply_keeps_body() {
        // A reply's payload is helper owned, so unknown fields must survive.
        let event: HelperEvent =
            serde_json::from_str(r#"{"kind":"reply","id":3,"entries":[],"path":"."}"#)
                .expect("reply parses");
        let HelperEvent::Reply { id, body } = event else {
            panic!("expected a reply");
        };
        assert_eq!(id, 3);
        assert_eq!(body["path"], ".");
        assert!(body.contains_key("entries"));
    }

    #[test]
    fn rejects_unknown_kind() {
        assert!(serde_json::from_str::<HelperEvent>(r#"{"kind":"nope"}"#).is_err());
    }
}
