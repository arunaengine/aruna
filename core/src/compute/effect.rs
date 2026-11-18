//! The calls an operation makes to the executor backends of this node, and their answers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;

use super::session::EndReason;
use super::{
    AttemptStatus, BackendError, CancelEvidence, ExecutorKind, FenceContext, GrantedResources,
    LogLimits, LogTails, ReconcileEvidence, TaskSpec, TombstoneEvidence, TombstoneSpec, UserSpec,
};
use crate::stream::{BackendStream, StreamError};
use crate::structs::placement::policy::PlacementSubject;

/// One call to the executor backends of this node.
#[derive(Debug, PartialEq)]
pub enum ComputeEffect {
    /// Picks the backend for a run, optionally of one kind.
    Select { constraint: Option<ExecutorKind> },
    /// What the backend of `kind` offers.
    Facts { kind: ExecutorKind },
    /// The backend of `kind`, while this node still advertises the exact site a launch was
    /// receipted under.
    Fenced {
        kind: ExecutorKind,
        subject: Box<PlacementSubject>,
        generation: u64,
        digest: [u8; 32],
    },
    /// The S3 endpoints a container reaches this node under.
    Endpoint,
    /// Pins an image; a job stop ends the call.
    ResolveImage { kind: ExecutorKind, image: String },
    /// The limits the backend enforces for exactly this task. The task comes back with them.
    Granted {
        kind: ExecutorKind,
        spec: Box<TaskSpec>,
    },
    /// One call about a fenced attempt.
    Attempt {
        kind: ExecutorKind,
        fence: FenceContext,
        request: ComputeRequest,
    },
}

/// What a call about a fenced attempt asks of the backend.
#[derive(Debug, PartialEq)]
pub enum ComputeRequest {
    Fence,
    /// Deletes the external object; only after its terminal evidence is durable.
    Cleanup,
    Tombstone(TombstoneSpec),
    /// Submits the attempt; a job stop cancels the call.
    Submit(Box<TaskSpec>),
    Cancel,
    Reconcile,
    FetchLogs(LogLimits),
    /// Expands a declared output pattern; with `until_stop` a job stop ends the call.
    ListOutputs {
        pattern: String,
        until_stop: bool,
    },
    /// Opens one output file; with `until_stop` a job stop ends the call and its bytes.
    FetchOutput {
        path: String,
        until_stop: bool,
    },
    /// Waits for whichever comes first: the attempt's end, the walltime, or the stop of the
    /// session of `session_job`. A job stop cancels the attempt wait.
    Await {
        walltime_left: Duration,
        session_job: Option<String>,
    },
    /// Registers the interactive session of the attempt.
    OpenSession(Box<SessionOpen>),
}

/// What an interactive session opens with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionOpen {
    pub job_id: String,
    pub public_job_id: String,
    pub runtime: String,
    pub workspace_bucket: String,
    pub executor_node_id: String,
    pub idle_after_ms: u64,
    pub credential_expires_ms: u64,
}

/// What one backend offers a run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackendFacts {
    pub kind: ExecutorKind,
    pub s3_mount: bool,
    pub session: bool,
    /// The workload runs on the controller host and inherits its execution subject.
    pub local_site: bool,
    /// Where the workers of a non-local backend run, when that is declared.
    pub worker_location: Option<String>,
    pub run_as: UserSpec,
}

/// The S3 endpoints a container reaches this node under.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkspaceAddress {
    pub endpoint: Option<String>,
    pub session_endpoint: Option<String>,
    pub region: String,
}

/// Why the wait for an attempt ended.
#[derive(Debug, PartialEq)]
pub enum AttemptEnd {
    Attempt(Result<AttemptStatus, BackendError>),
    Walltime,
    /// The client ended the session, it went idle, or its kernel exited.
    SessionStopped(EndReason),
}

/// The error an output stream failed with, kept for whoever reads the stream.
#[derive(Clone, Debug, Default)]
pub struct OutputFailure(Arc<Mutex<Option<BackendError>>>);

impl OutputFailure {
    pub fn record(&self, error: BackendError) {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = Some(error);
    }

    pub fn take(&self) -> Option<BackendError> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).take()
    }
}

/// Two handles are equal when they share their slot.
impl PartialEq for OutputFailure {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

/// One opened output file: its bytes, and where a failure of the stream is kept.
#[derive(Debug, PartialEq)]
pub struct OutputBody {
    pub size: u64,
    pub bytes: BackendStream<Result<Bytes, StreamError>>,
    pub failure: OutputFailure,
}

/// The answer to one [`ComputeEffect`].
#[derive(Debug, PartialEq)]
pub enum ComputeAnswer {
    /// This node has no compute plane.
    NoCompute,
    /// This node has no backend of that kind.
    NoBackend,
    /// The job stopped before the call ended.
    Stopped,
    Selected(Option<BackendFacts>),
    Facts(BackendFacts),
    Fenced(Result<BackendFacts, BackendError>),
    Endpoint(WorkspaceAddress),
    Image(Result<String, BackendError>),
    Granted {
        spec: Box<TaskSpec>,
        resources: Result<GrantedResources, BackendError>,
    },
    Done(Result<(), BackendError>),
    Tombstone(Result<TombstoneEvidence, BackendError>),
    Submitted(Result<AttemptStatus, BackendError>),
    Cancelled(Result<CancelEvidence, BackendError>),
    Reconciled(ReconcileEvidence),
    Logs(Result<LogTails, BackendError>),
    Outputs(Result<Vec<String>, BackendError>),
    Output(Result<OutputBody, BackendError>),
    Ended(AttemptEnd),
}
