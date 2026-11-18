//! Ends an execution run in the store: terminal writes, the park of an unresolved attempt, and
//! the announcements that follow a terminal state.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::LogTails;
use aruna_core::effects::Effect;
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{
    JobError, JobErrorKind, JobId, JobPayload, JobProgress, JobRecord, JobResultPayload, JobState,
    OutputObject,
};
use aruna_core::structs::identity::realm::RealmId;
use tracing::{info, warn};
use ulid::Ulid;

use super::prepare::job_bucket;
use crate::jobs::lifecycle::LifecycleError;
use crate::jobs::lifecycle::updates::{
    PublishTerminalOperation, SETTLE_RETRY_AFTER, schedule_terminal_settle,
};
use crate::jobs::store::JobMutationError;
use crate::jobs::store::job_calls::{
    CancelRequestOutcome, ParkOutcome, RunSuccess, cancel_execution, complete_cancelled,
    fail_execution, mark_indeterminate, requeue_before_attempt, set_cancel_requested,
};
use crate::jobs::store::row_calls::read_job_record;
use crate::jobs::submit::schedule_drain_effect;

/// The clock and the node a settling run works with.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SettleScope {
    pub serving: Option<(NodeId, RealmId)>,
    pub now_ms: u64,
}

fn task_call(effect: Effect) -> IoCall {
    IoCall::Effect(Box::new(effect))
}

/// Announces a terminal state this run wrote: a receipted execution publishes it and frees its
/// reservation; a publication that failed goes to the settle task, whose reservation row keeps
/// the obligation durable. The drain then picks up the follow-on jobs.
#[derive(Debug, PartialEq)]
pub struct AnnounceTerminalOperation {
    record: Option<JobRecord>,
    scope: SettleScope,
}

impl AnnounceTerminalOperation {
    pub fn new(record: JobRecord, scope: SettleScope) -> Self {
        Self {
            record: Some(record),
            scope,
        }
    }
}

impl Operation for AnnounceTerminalOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        let Some(record) = self.record.take() else {
            return Ok(Next::Done(()));
        };
        log_compute_summary(&record);
        let publish = PublishTerminalOperation::new(record, self.scope.serving, self.scope.now_ms);
        Ok(Next::Call(IoCall::child(publish)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        let published = match result.child::<bool, LifecycleError>()? {
            Ok(published) => published,
            Err(error) => {
                warn!(error = %error, "Execution publication failed");
                false
            }
        };
        let mut calls = Vec::with_capacity(2);
        if !published {
            calls.push(task_call(schedule_terminal_settle(SETTLE_RETRY_AFTER)));
        }
        calls.push(task_call(schedule_drain_effect()));
        Ok(Next::Then { output: (), calls })
    }
}

fn log_compute_summary(record: &JobRecord) {
    let JobPayload::Execution(spec) = &record.payload else {
        return;
    };
    let intent = record.attempt_intent.as_ref();
    let attempt = intent.map_or(record.attempts, |intent| intent.attempt_no);
    let executor_kind = intent
        .map(|intent| intent.executor_kind.as_str())
        .or(spec.executor_constraint.as_deref())
        .unwrap_or("unresolved");
    let outcome = match record.state {
        JobState::Succeeded => "success",
        JobState::Failed => "failure",
        JobState::Cancelled => "cancelled",
        _ => return,
    };
    info!(
        event = "pipeline.compute.summary",
        job_id = %record.job_id,
        attempt,
        image = %spec.image,
        executor_kind,
        state = record.state.name(),
        outcome,
        "Compute job summary"
    );
}

/// The terminal state a run ends in, with the evidence it keeps.
#[derive(Debug, PartialEq)]
pub enum RunEnding {
    Fail {
        error: JobError,
        result: JobResultPayload,
    },
    Cancel {
        result: JobResultPayload,
    },
    /// Success of a run whose cancel came too late to stop it.
    Complete {
        result: JobResultPayload,
    },
}

#[derive(Debug, PartialEq)]
enum EndStep {
    Progress,
    Write,
    Announce,
}

/// Writes the terminal state of a run and, when this run wrote it, announces it. A write that
/// lost its race is logged and announces nothing.
#[derive(Debug, PartialEq)]
pub struct EndRunOperation {
    job_id: JobId,
    token: Ulid,
    ending: Option<RunEnding>,
    /// What the log says when the terminal write fails.
    failure: &'static str,
    scope: SettleScope,
    step: EndStep,
}

impl EndRunOperation {
    pub fn new(job_id: JobId, token: Ulid, ending: RunEnding, scope: SettleScope) -> Self {
        let failure = match &ending {
            RunEnding::Fail { .. } => "Execution fail write failed",
            RunEnding::Cancel { .. } => "Execution cancel write failed",
            RunEnding::Complete { .. } => "Execution complete write failed",
        };
        Self {
            job_id,
            token,
            ending: Some(ending),
            failure,
            scope,
            step: EndStep::Write,
        }
    }

    fn write(&mut self, progress: Option<JobProgress>) -> Result<Next<()>, IoError> {
        let (job_id, token, now_ms) = (self.job_id, self.token, self.scope.now_ms);
        self.step = EndStep::Write;
        let call = match self.ending.take() {
            Some(RunEnding::Fail { error, result }) => {
                IoCall::child(fail_execution(job_id, token, error, result, now_ms))
            }
            Some(RunEnding::Cancel { result }) => {
                IoCall::child(cancel_execution(job_id, token, result, now_ms))
            }
            Some(RunEnding::Complete { result }) => {
                let progress = progress.unwrap_or_else(|| JobProgress::new("phases"));
                let success = RunSuccess {
                    result,
                    progress,
                    now_ms,
                };
                IoCall::child(complete_cancelled(job_id, token, success))
            }
            None => return Ok(Next::Done(())),
        };
        Ok(Next::Call(call))
    }
}

impl Operation for EndRunOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        if matches!(self.ending, Some(RunEnding::Complete { .. })) {
            self.step = EndStep::Progress;
            return Ok(Next::Call(IoCall::child(read_job_record(self.job_id))));
        }
        self.write(None)
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        match self.step {
            EndStep::Progress => {
                let record = result.child::<Option<JobRecord>, JobMutationError>()?;
                self.write(record.ok().flatten().map(|record| record.progress))
            }
            EndStep::Write => match result.child::<JobRecord, JobMutationError>()? {
                Ok(record) => {
                    self.step = EndStep::Announce;
                    let announce = AnnounceTerminalOperation::new(record, self.scope);
                    Ok(Next::Call(IoCall::child(announce)))
                }
                Err(error) => {
                    let job_id = self.job_id;
                    warn!(job_id = %job_id, error = %error, "{}", self.failure);
                    Ok(Next::Done(()))
                }
            },
            EndStep::Announce => {
                result.child::<(), IoError>()??;
                Ok(Next::Done(()))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum ParkStep {
    Mark,
    Cancel(Box<JobRecord>),
    Announce,
}

/// Parks an ambiguous attempt for a later reconcile pass. The park charges an attempt, so a
/// failure that repeats on every pass terminalizes at the cap instead of driving the job forever.
#[derive(Debug, PartialEq)]
pub struct ParkAttemptOperation {
    job_id: JobId,
    token: Ulid,
    error: Option<JobError>,
    scope: SettleScope,
    step: ParkStep,
}

impl ParkAttemptOperation {
    pub fn new(job_id: JobId, token: Ulid, error: JobError, scope: SettleScope) -> Self {
        Self {
            job_id,
            token,
            error: Some(error),
            scope,
            step: ParkStep::Mark,
        }
    }

    fn announce(&mut self, record: JobRecord) -> Next<()> {
        self.step = ParkStep::Announce;
        Next::Call(IoCall::child(AnnounceTerminalOperation::new(
            record, self.scope,
        )))
    }
}

impl Operation for ParkAttemptOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        let Some(error) = self.error.take() else {
            return Ok(Next::Done(()));
        };
        let mark = mark_indeterminate(self.job_id, self.token, error, self.scope.now_ms);
        Ok(Next::Call(IoCall::child(mark)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        let job_id = self.job_id;
        match std::mem::replace(&mut self.step, ParkStep::Announce) {
            ParkStep::Mark => match result.child::<ParkOutcome, JobMutationError>()? {
                Ok(ParkOutcome::Parked(_)) => Ok(Next::Done(())),
                Ok(ParkOutcome::Exhausted(record)) => {
                    warn!(job_id = %job_id, "Local attempts exhausted; parking job");
                    // A cancelled run settles as cancelled, so the family does not run it
                    // elsewhere.
                    if !record.cancel_requested {
                        return Ok(self.announce(record));
                    }
                    self.step = ParkStep::Cancel(Box::new(record));
                    let cancel = set_cancel_requested(job_id, self.scope.now_ms);
                    Ok(Next::Call(IoCall::child(cancel)))
                }
                Err(error) => {
                    warn!(job_id = %job_id, error = %error, "Attempt park write failed");
                    Ok(Next::Done(()))
                }
            },
            ParkStep::Cancel(record) => {
                let cancelled = result.child::<CancelRequestOutcome, JobMutationError>()?;
                Ok(self.announce(match cancelled {
                    Ok(CancelRequestOutcome::Cancelled(cancelled)) => cancelled,
                    _ => *record,
                }))
            }
            ParkStep::Announce => {
                result.child::<(), IoError>()??;
                Ok(Next::Done(()))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum FailureStep {
    End,
    Requeue,
    Announce,
}

/// Handles a failure before any attempt was submitted. A permanent error ends the job; any
/// other sends it back to the queue, which ends it once its attempts are spent.
#[derive(Debug, PartialEq)]
pub struct SubmitFailureOperation {
    job_id: JobId,
    token: Ulid,
    bucket: String,
    error: Option<JobError>,
    logged: bool,
    scope: SettleScope,
    step: FailureStep,
}

impl SubmitFailureOperation {
    /// `logged` says the caller already reported this error.
    pub fn new(
        record: &JobRecord,
        token: Ulid,
        error: JobError,
        logged: bool,
        scope: SettleScope,
    ) -> Self {
        Self {
            job_id: record.job_id,
            token,
            bucket: job_bucket(record),
            error: Some(error),
            logged,
            scope,
            step: FailureStep::End,
        }
    }
}

impl Operation for SubmitFailureOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        let job_id = self.job_id;
        let Some(error) = self.error.take() else {
            return Ok(Next::Done(()));
        };
        if error.kind != JobErrorKind::Permanent {
            self.step = FailureStep::Requeue;
            let requeue = requeue_before_attempt(job_id, self.token, self.scope.now_ms, error);
            return Ok(Next::Call(IoCall::child(requeue)));
        }
        if !self.logged {
            warn!(job_id = %job_id, error = ?error, "Permanent pre-submit failure");
        }
        let result = execution_result(&self.bucket, None, Vec::new(), LogTails::default());
        let ending = RunEnding::Fail { error, result };
        let end = EndRunOperation::new(job_id, self.token, ending, self.scope);
        Ok(Next::Call(IoCall::child(end)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        let job_id = self.job_id;
        match self.step {
            FailureStep::Requeue => match result.child::<JobRecord, JobMutationError>()? {
                Ok(record) if record.is_settled() => {
                    self.step = FailureStep::Announce;
                    let announce = AnnounceTerminalOperation::new(record, self.scope);
                    Ok(Next::Call(IoCall::child(announce)))
                }
                Ok(_) => Ok(Next::Done(())),
                Err(error) => {
                    warn!(job_id = %job_id, error = %error, "Pre-submit requeue failed");
                    Ok(Next::Done(()))
                }
            },
            FailureStep::End | FailureStep::Announce => {
                result.child::<(), IoError>()??;
                Ok(Next::Done(()))
            }
        }
    }
}

/// The result an execution records, with the tails of its logs.
pub(super) fn execution_result(
    bucket: &str,
    exit_code: Option<i32>,
    outputs: Vec<OutputObject>,
    logs: LogTails,
) -> JobResultPayload {
    JobResultPayload::Execution {
        exit_code,
        workspace_bucket: (!bucket.is_empty()).then(|| bucket.to_string()),
        outputs,
        stdout: log_tail(logs.stdout, logs.stdout_truncated),
        stderr: log_tail(logs.stderr, logs.stderr_truncated),
        output_digest: None,
    }
}

fn log_tail(bytes: Vec<u8>, truncated: bool) -> String {
    let tail = String::from_utf8_lossy(&bytes);
    if truncated {
        format!("[truncated]\n{tail}")
    } else {
        tail.into_owned()
    }
}

#[cfg(test)]
#[path = "settle_tests.rs"]
mod tests;
