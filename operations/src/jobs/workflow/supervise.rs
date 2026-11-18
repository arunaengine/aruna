//! Waits for a running attempt, its walltime or its session, then finalizes the outcome.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::effect::{
    AttemptEnd, ComputeAnswer, ComputeEffect, ComputeRequest, SessionOpen,
};
use aruna_core::compute::session::{EndReason, SessionAnswer, SessionRequest};
use aruna_core::compute::{AttemptStatus, BackendError};
use aruna_core::events::Event;
use aruna_core::jobs::JobEvent;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{JobId, JobRecord};
use tracing::warn;
use ulid::Ulid;

use super::DEFAULT_WALLTIME;
use super::attempt::{AttemptScope, stops_call};
use super::compute::{compute_answer, compute_call};
use super::finalize::{FinalizeGoal, FinalizeOperation};
use super::session::{
    SessionInputs, SessionPlan, SessionPlanOperation, SessionReportOperation, session_call,
};
use crate::jobs::lifecycle::ids::session_of;
use crate::jobs::store::job_calls::record_attempt_started;
use crate::jobs::store::mutation_error::JobMutationError;
use crate::jobs::store::row_calls::read_job_record;

#[derive(Debug, PartialEq)]
enum SuperviseStep {
    Anchor,
    Facts(u64),
    Plan(u64),
    Open(u64),
    Await,
    Stops(Box<Result<AttemptStatus, BackendError>>),
    Ended(FinalizeWork),
    Reported(FinalizeWork),
    Finalize,
    Close,
}

/// What supervision finalizes once the session bookkeeping is done.
#[derive(Debug, PartialEq)]
enum FinalizeWork {
    Attempt(Box<Result<AttemptStatus, BackendError>>),
    Walltime,
}

type SuperviseNext = Result<Next<()>, IoError>;

/// Waits for the end of a running attempt and settles it from the evidence. Shared by the fresh
/// path and by adoption. The caller keeps the lease alive meanwhile.
#[derive(Debug, PartialEq)]
pub struct SuperviseOperation {
    scope: Arc<AttemptScope>,
    /// Whether a session of this job was opened here.
    session: bool,
    /// The reason the session is being ended for.
    pending_reason: Option<EndReason>,
    now_ms: u64,
    step: SuperviseStep,
}

impl SuperviseOperation {
    pub fn new(scope: Arc<AttemptScope>) -> Self {
        Self {
            scope,
            session: false,
            pending_reason: None,
            now_ms: 0,
            step: SuperviseStep::Anchor,
        }
    }

    fn call(&mut self, step: SuperviseStep, call: IoCall) -> SuperviseNext {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn job_key(&self) -> String {
        self.scope.job_id.to_string()
    }

    /// The walltime cap needs an anchor even without backend start evidence, and the first
    /// pass persists it, so a restarted supervisor cannot hand the attempt a fresh window.
    fn anchored(&mut self, started_at_ms: u64) -> SuperviseNext {
        let scope = self.scope.clone();
        if session_of(&scope.spec).is_none() || scope.wiring.serving.is_none() {
            return self.wait(started_at_ms);
        }
        let kind = scope.kind.clone();
        let call = compute_call(ComputeEffect::Facts { kind });
        self.call(SuperviseStep::Facts(started_at_ms), call)
    }

    fn plan_session(&mut self, started_at_ms: u64, mounts: bool) -> SuperviseNext {
        let scope = self.scope.clone();
        let (Some(requested), Some((node_id, realm_id))) =
            (session_of(&scope.spec), scope.wiring.serving)
        else {
            return self.wait(started_at_ms);
        };
        let inputs = SessionInputs {
            realm_id,
            node_id,
            mounts,
            encryption_key: scope.wiring.encryption_key.clone(),
            now: SystemTime::UNIX_EPOCH + Duration::from_millis(self.now_ms),
        };
        let plan = SessionPlanOperation::new(
            scope.job_id,
            &scope.spec,
            &scope.bucket,
            requested.idle_after_ms,
            inputs,
        );
        self.call(SuperviseStep::Plan(started_at_ms), IoCall::child(plan))
    }

    fn open_session(&mut self, started_at_ms: u64, plan: SessionPlan) -> SuperviseNext {
        let scope = self.scope.clone();
        let (Some(requested), Some((node_id, _))) = (session_of(&scope.spec), scope.wiring.serving)
        else {
            return self.wait(started_at_ms);
        };
        let open = SessionOpen {
            job_id: scope.job_id.to_string(),
            public_job_id: plan.public_job_id.to_string(),
            runtime: requested.runtime,
            workspace_bucket: scope.bucket.clone(),
            executor_node_id: node_id.to_string(),
            idle_after_ms: plan.idle_after_ms,
            credential_expires_ms: plan.credential_expires_ms,
        };
        let call = scope.backend(ComputeRequest::OpenSession(Box::new(open)));
        self.call(SuperviseStep::Open(started_at_ms), call)
    }

    fn wait(&mut self, started_at_ms: u64) -> SuperviseNext {
        let scope = self.scope.clone();
        let walltime_ms =
            (scope.spec.resources.max_walltime_ms).unwrap_or(DEFAULT_WALLTIME.as_millis() as u64);
        let left = (started_at_ms.saturating_add(walltime_ms)).saturating_sub(self.now_ms);
        let request = ComputeRequest::Await {
            walltime_left: Duration::from_millis(left),
            session_job: self.session.then(|| self.job_key()),
        };
        self.call(SuperviseStep::Await, scope.backend(request))
    }

    fn finalize(&mut self, goal: FinalizeGoal) -> SuperviseNext {
        let finalize = FinalizeOperation::new(self.scope.clone(), goal);
        self.call(SuperviseStep::Finalize, IoCall::child(finalize))
    }

    /// Ends the session for `reason`, then reports it before the attempt is finalized.
    fn end_session(&mut self, reason: EndReason, work: FinalizeWork) -> SuperviseNext {
        let call = session_call(self.scope.job_id, SessionRequest::End(reason));
        self.pending_reason = Some(reason);
        self.call(SuperviseStep::Ended(work), call)
    }

    fn finalize_work(&mut self, work: FinalizeWork) -> SuperviseNext {
        match work {
            FinalizeWork::Attempt(result) => self.finalize(FinalizeGoal::Attempt(*result)),
            FinalizeWork::Walltime => self.finalize(FinalizeGoal::Walltime),
        }
    }

    /// The session outlives its end until here, so a client that reads or ends it during
    /// teardown still gets the ended state.
    fn close(&mut self) -> SuperviseNext {
        if !self.session {
            return Ok(Next::Done(()));
        }
        let call = session_call(self.scope.job_id, SessionRequest::Close);
        self.call(SuperviseStep::Close, call)
    }
}

impl Operation for SuperviseOperation {
    type Output = ();
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> SuperviseNext {
        let scope = &self.scope;
        let anchor = WalltimeAnchorOperation::new(scope.job_id, scope.token);
        self.call(SuperviseStep::Anchor, IoCall::child(anchor))
    }

    fn next(&mut self, result: IoResult) -> SuperviseNext {
        let scope = self.scope.clone();
        match std::mem::replace(&mut self.step, SuperviseStep::Close) {
            SuperviseStep::Anchor => {
                let anchored = result.child::<u64, IoError>()?;
                self.anchored(anchored.unwrap_or(self.now_ms))
            }
            SuperviseStep::Facts(started_at_ms) => {
                let mounts = match compute_answer(result)? {
                    ComputeAnswer::Facts(facts) => facts.s3_mount,
                    _ => false,
                };
                self.plan_session(started_at_ms, mounts)
            }
            SuperviseStep::Plan(started_at_ms) => {
                match result.child::<Option<SessionPlan>, IoError>()? {
                    Ok(Some(plan)) => self.open_session(started_at_ms, plan),
                    _ => self.wait(started_at_ms),
                }
            }
            SuperviseStep::Open(started_at_ms) => {
                self.session = matches!(compute_answer(result)?, ComputeAnswer::Done(Ok(())));
                self.wait(started_at_ms)
            }
            SuperviseStep::Await => match compute_answer(result)? {
                ComputeAnswer::Ended(AttemptEnd::Attempt(ended)) if self.session => {
                    self.call(SuperviseStep::Stops(Box::new(ended)), stops_call())
                }
                ComputeAnswer::Ended(AttemptEnd::Attempt(ended)) => {
                    self.finalize(FinalizeGoal::Attempt(ended))
                }
                ComputeAnswer::Ended(AttemptEnd::Walltime) if self.session => {
                    self.end_session(EndReason::Walltime, FinalizeWork::Walltime)
                }
                ComputeAnswer::Ended(AttemptEnd::Walltime) => self.finalize(FinalizeGoal::Walltime),
                ComputeAnswer::Ended(AttemptEnd::SessionStopped(reason)) => {
                    self.finalize(FinalizeGoal::Session(reason))
                }
                _ => self.finalize(FinalizeGoal::Attempt(Err(BackendError::Unavailable(
                    "the attempt's backend is gone".to_string(),
                )))),
            },
            // A cancelled attempt ends its session for that reason; any other return means the
            // container stopped on its own.
            SuperviseStep::Stops(ended) => {
                let reason = match result.event()? {
                    Event::Job(JobEvent::Stops(stops)) if stops.cancelled => EndReason::Cancelled,
                    _ => EndReason::KernelExit,
                };
                self.end_session(reason, FinalizeWork::Attempt(ended))
            }
            SuperviseStep::Ended(work) => {
                let _ = result.child::<SessionAnswer, IoError>()?;
                let reason = self.pending_reason.take().ok_or(IoError::Unfinished)?;
                let report = SessionReportOperation::new(scope.job_id, scope.token, true, reason);
                self.call(SuperviseStep::Reported(work), IoCall::child(report))
            }
            SuperviseStep::Reported(work) => {
                let _ = result.child::<(), IoError>()?;
                self.finalize_work(work)
            }
            SuperviseStep::Finalize => {
                if let Err(error) = result.child::<(), IoError>()? {
                    warn!(job_id = %scope.job_id, %error, "Attempt finalization failed");
                }
                self.close()
            }
            SuperviseStep::Close => {
                let _ = result.child::<SessionAnswer, IoError>()?;
                Ok(Next::Done(()))
            }
        }
    }
}

/// Finds when an attempt's walltime started. The cap needs an anchor even without backend start
/// evidence, and the first pass persists the one it derives, so a restarted supervisor cannot
/// hand the attempt a fresh window.
#[derive(Debug, PartialEq)]
pub struct WalltimeAnchorOperation {
    job_id: JobId,
    token: Ulid,
    anchor: Option<u64>,
    now_ms: u64,
}

impl WalltimeAnchorOperation {
    pub fn new(job_id: JobId, token: Ulid) -> Self {
        Self {
            job_id,
            token,
            anchor: None,
            now_ms: 0,
        }
    }
}

impl Operation for WalltimeAnchorOperation {
    type Output = u64;
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<u64>, IoError> {
        Ok(Next::Call(IoCall::child(read_job_record(self.job_id))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<u64>, IoError> {
        let Some(anchor) = self.anchor else {
            let record = result.child::<Option<JobRecord>, JobMutationError>()?;
            if let Some(started) = record.ok().flatten().and_then(|job| job.started_at_ms) {
                return Ok(Next::Done(started));
            }
            self.anchor = Some(self.now_ms);
            let write = record_attempt_started(self.job_id, self.token, self.now_ms);
            return Ok(Next::Call(IoCall::child(write)));
        };
        Ok(Next::Done(
            match result.child::<JobRecord, JobMutationError>()? {
                Ok(record) => record.started_at_ms.unwrap_or(anchor),
                Err(error) => {
                    warn!(job_id = %self.job_id, error = %error, "Walltime anchor write failed");
                    anchor
                }
            },
        ))
    }
}

#[derive(Debug, PartialEq)]
enum AbandonStep {
    End,
    Close,
}

/// Stops the session of an execution another node took over: this node no longer owns the
/// attempt, so nothing else would deliver an end for its session.
#[derive(Debug, PartialEq)]
pub struct AbandonSessionOperation {
    job_id: JobId,
    step: AbandonStep,
}

impl AbandonSessionOperation {
    pub fn new(job_id: JobId) -> Self {
        Self {
            job_id,
            step: AbandonStep::End,
        }
    }
}

impl Operation for AbandonSessionOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        let end = SessionRequest::End(EndReason::Cancelled);
        Ok(Next::Call(session_call(self.job_id, end)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        let _ = result.child::<SessionAnswer, IoError>()?;
        match self.step {
            AbandonStep::End => {
                self.step = AbandonStep::Close;
                Ok(Next::Call(session_call(self.job_id, SessionRequest::Close)))
            }
            AbandonStep::Close => Ok(Next::Done(())),
        }
    }
}

#[cfg(test)]
#[path = "supervise_tests.rs"]
mod tests;
