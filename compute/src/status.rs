use serde::{Deserialize, Serialize};

use crate::backend::BackendError;

/// Observed lifecycle of one attempt. `Exited`/`Failed`/`Cancelled` are
/// definitive terminal evidence; `Submitted`/`Running` are not.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AttemptPhase {
    /// Accepted by the backend, not yet running.
    Submitted,
    Running,
    /// Definitive terminal evidence: the process exited with this code.
    Exited {
        code: i32,
    },
    /// Backend-level failure (image pull, OOM-kill, node lost).
    Failed {
        reason: String,
    },
    /// Definitive terminal evidence: stopped due to our cancel.
    Cancelled,
}

impl AttemptPhase {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            AttemptPhase::Exited { .. } | AttemptPhase::Failed { .. } | AttemptPhase::Cancelled
        )
    }

    /// Projection onto the GA4GH TES external state. `Queued` (pre-submit) and
    /// `Unknown` (Indeterminate) are Job-side states and never come from a phase.
    pub fn tes_state(&self) -> TesState {
        match self {
            AttemptPhase::Submitted => TesState::Initializing,
            AttemptPhase::Running => TesState::Running,
            AttemptPhase::Exited { code: 0 } => TesState::Complete,
            AttemptPhase::Exited { .. } => TesState::ExecutorError,
            AttemptPhase::Failed { .. } => TesState::SystemError,
            AttemptPhase::Cancelled => TesState::Canceled,
        }
    }
}

/// GA4GH TES external task state. The Stage-2 Aruna side maps this (plus the
/// Job-side `Queued`/`Unknown`) onto the durable Job state machine.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TesState {
    Queued,
    Initializing,
    Running,
    Complete,
    ExecutorError,
    SystemError,
    Canceled,
    Unknown,
}

/// A point-in-time observation of an attempt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptStatus {
    pub phase: AttemptPhase,
    /// Container id / K8s job uid / Slurm job id.
    pub backend_ref: String,
    pub started_at_ms: Option<u64>,
    pub finished_at_ms: Option<u64>,
}

impl AttemptStatus {
    pub fn is_terminal(&self) -> bool {
        self.phase.is_terminal()
    }
}

/// Result of querying by deterministic name after restart / lease loss.
/// Never mutates the backend.
#[derive(Debug)]
pub enum ReconcileOutcome {
    /// The attempt object exists; adopt it.
    Found(AttemptStatus),
    /// No object with the deterministic name exists.
    NotFound,
    /// Backend unreachable. NOT evidence: the caller parks in Indeterminate.
    Unavailable(BackendError),
}

/// Evidence returned by a cancel request.
#[derive(Debug)]
pub enum CancelEvidence {
    /// Definitive: the attempt is stopped.
    Stopped(AttemptStatus),
    /// The stop was requested but no terminal evidence is available yet.
    Requested,
    /// No object with the deterministic name exists.
    AlreadyGone,
}
