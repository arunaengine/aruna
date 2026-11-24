//! The calls an operation makes to the workflow coordinators of this node, and their answers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_workflow_contract::{
    ContractError, CoordinatorMode, CoordinatorToHost, HostToCoordinator, PreflightKind,
    PreflightResult,
};
use async_trait::async_trait;

/// One call to a workflow coordinator of this node; never persisted.
#[derive(Debug, PartialEq)]
pub enum WorkflowEffect {
    /// One request to a warm preflight coordinator, which runs no task.
    Preflight(PreflightKind),
    /// Starts a coordinator attempt for a run or a report and greets it.
    Open {
        session: String,
        mode: CoordinatorMode,
        run_id: String,
        claim_token_digest: String,
    },
    /// Sends one message to an open session.
    Send {
        session: String,
        message: HostToCoordinator,
    },
    /// Waits for the next message of an open session.
    Receive { session: String },
    /// Ends a session and its coordinator attempt.
    Close { session: String },
}

/// The answer to one [`WorkflowEffect`].
#[derive(Debug, PartialEq)]
pub enum WorkflowAnswer {
    Preflight(Result<PreflightResult, WorkflowError>),
    Opened(Result<(), WorkflowError>),
    Sent(Result<(), WorkflowError>),
    /// The next message; `None` when the coordinator ended the channel cleanly.
    Received(Result<Option<CoordinatorToHost>, WorkflowError>),
    Closed(Result<(), WorkflowError>),
}

/// Why a coordinator call failed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum WorkflowError {
    #[error("this node runs no workflow coordinator")]
    Unavailable,
    #[error("the coordinator did not answer within {seconds} seconds")]
    Timeout { seconds: u64 },
    /// The coordinator ended or broke its channel; another attempt starts a new one.
    #[error("the coordinator failed: {0}")]
    Crashed(String),
    #[error("no open coordinator session {0}")]
    UnknownSession(String),
    /// The coordinator answered the request with a refusal of its own.
    #[error(transparent)]
    Refused(ContractError),
}

/// The workflow runtime of a node, as the runner reaches it.
#[async_trait]
pub trait WorkflowHost: Send + Sync {
    async fn serve(&self, effect: WorkflowEffect) -> WorkflowAnswer;
}

impl WorkflowEffect {
    /// The answer of a node without a workflow runtime.
    pub fn unavailable(&self) -> WorkflowAnswer {
        let refused = Err(WorkflowError::Unavailable);
        match self {
            WorkflowEffect::Preflight(_) => {
                WorkflowAnswer::Preflight(Err(WorkflowError::Unavailable))
            }
            WorkflowEffect::Open { .. } => WorkflowAnswer::Opened(refused),
            WorkflowEffect::Send { .. } => WorkflowAnswer::Sent(refused),
            WorkflowEffect::Receive { .. } => {
                WorkflowAnswer::Received(Err(WorkflowError::Unavailable))
            }
            WorkflowEffect::Close { .. } => WorkflowAnswer::Closed(refused),
        }
    }
}
