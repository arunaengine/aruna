//! Recovers an ambiguous attempt submit by retrying the same name, retiring, or parking it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::effect::{ComputeAnswer, ComputeRequest};
use aruna_core::compute::{
    AttemptStatus, BackendError, ReconcileEvidence, TombstoneEvidence, TombstoneSpec,
};
use aruna_core::events::Event;
use aruna_core::jobs::JobEvent;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{AttemptControl, JobError, JobRecord};
use tracing::warn;

use super::attempt::{AttemptScope, stops_call};
use super::compute::{RecoveryAction, compute_answer, recovery_action};
use super::finalize::{FinalizeGoal, FinalizeOperation};
use super::prepare::{PrepareAttemptOperation, PreparedTask, build_task_spec};
use super::settle::{ParkAttemptOperation, SubmitFailureOperation};
use crate::jobs::store::JobMutationError;
use crate::jobs::store::job_calls::{begin_external_running, record_attempt_tombstone};
use crate::jobs::store::row_calls::read_job_record;

/// The job error of a submit that failed for good or for now.
pub(super) fn submit_error(error: &BackendError) -> JobError {
    let message = format!("submit failed: {error}");
    match error.retryable() {
        true => JobError::retryable(message),
        false => JobError::permanent(message),
    }
}

#[derive(Debug, PartialEq)]
enum RecoverStep {
    Stops,
    Record,
    Reconcile,
    Running {
        finalize: Option<Box<AttemptStatus>>,
    },
    Prepare,
    Submit,
    Tombstone,
    Recorded,
    StopThenPark,
    /// A child that ends the recovery with `false`.
    Last,
}

type RecoverNext = Result<Next<bool>, IoError>;

/// A submit error after the write-ahead intent is ambiguous: the container may already exist or
/// run. Requeueing would erase the intent and launch a second container under the next attempt
/// name, so the deterministic name is used instead. Answers whether supervision goes on.
#[derive(Debug, PartialEq)]
pub struct RecoverSubmitOperation {
    scope: Arc<AttemptScope>,
    error: BackendError,
    record: Option<Arc<JobRecord>>,
    now_ms: u64,
    step: RecoverStep,
}

impl RecoverSubmitOperation {
    pub fn new(scope: Arc<AttemptScope>, error: BackendError) -> Self {
        Self {
            scope,
            error,
            record: None,
            now_ms: 0,
            step: RecoverStep::Stops,
        }
    }

    fn call(&mut self, step: RecoverStep, call: IoCall) -> RecoverNext {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn cancel(&mut self) -> RecoverNext {
        let finalize = FinalizeOperation::new(self.scope.clone(), FinalizeGoal::Cancel);
        self.call(RecoverStep::Last, IoCall::child(finalize))
    }

    fn park(&mut self, error: JobError) -> RecoverNext {
        let scope = &self.scope;
        let settle = scope.settle(self.now_ms);
        let park = ParkAttemptOperation::new(scope.job_id, scope.token, error, settle);
        self.call(RecoverStep::Last, IoCall::child(park))
    }

    fn park_failed(&mut self, error: &BackendError) -> RecoverNext {
        self.park(JobError::retryable(format!(
            "submit failed ambiguously: {error}"
        )))
    }

    fn running(&mut self, status: &AttemptStatus, finalize: bool) -> RecoverNext {
        let scope = &self.scope;
        let write =
            begin_external_running(scope.job_id, scope.token, status.started_at_ms, self.now_ms);
        let finalize = finalize.then(|| Box::new(status.clone()));
        self.call(RecoverStep::Running { finalize }, IoCall::child(write))
    }

    fn retry_same(&mut self) -> RecoverNext {
        let scope = self.scope.clone();
        let record = self.record.clone().ok_or(IoError::Unfinished)?;
        let Some(run) = scope.run(&record) else {
            let error = BackendError::Unavailable("execution needs a net handle".to_string());
            return self.retry_failed(error);
        };
        let prepare = PrepareAttemptOperation::new(
            run,
            (scope.spec.clone(), record),
            &scope.bucket,
            scope.wiring.encryption_key.clone(),
            false,
        );
        self.call(RecoverStep::Prepare, IoCall::child(prepare))
    }

    fn submit(&mut self, prepared: PreparedTask) -> RecoverNext {
        let scope = self.scope.clone();
        let record = self.record.as_ref().ok_or(IoError::Unfinished)?;
        let pinned = (record.attempt_intent.as_ref())
            .filter(|intent| intent.attempt_epoch == scope.fence.attempt_epoch)
            .map(|intent| intent.pinned_image.clone());
        let Some(pinned) = pinned else {
            let error = BackendError::Conflict("attempt intent mismatch".to_string());
            return self.retry_failed(error);
        };
        let task = build_task_spec(
            &scope.spec,
            &scope.fence.attempt,
            &pinned,
            prepared,
            scope.run_as,
        );
        let call = scope.backend(ComputeRequest::Submit(Box::new(task)));
        self.call(RecoverStep::Submit, call)
    }

    fn retry_failed(&mut self, error: BackendError) -> RecoverNext {
        if error == BackendError::Cancelled {
            return self.cancel();
        }
        if error.retryable() {
            return self.park_failed(&error);
        }
        self.error = error;
        let spec = TombstoneSpec { terminal_ref: None };
        let call = self.scope.backend(ComputeRequest::Tombstone(spec));
        self.call(RecoverStep::Tombstone, call)
    }

    fn record_tombstone(&mut self, tombstone: TombstoneEvidence) -> RecoverNext {
        let scope = &self.scope;
        let write = record_attempt_tombstone(
            scope.job_id,
            scope.token,
            scope.fence.attempt_epoch,
            tombstone.backend_ref,
        );
        self.call(RecoverStep::Recorded, IoCall::child(write))
    }

    fn evidence(&mut self, evidence: ReconcileEvidence) -> RecoverNext {
        match recovery_action(&evidence) {
            RecoveryAction::Observe => {
                let ReconcileEvidence::Adoptable(evidence) = evidence else {
                    return Ok(Next::Done(false));
                };
                let status = evidence.status;
                self.running(&status, status.is_terminal())
            }
            RecoveryAction::RetrySame => self.retry_same(),
            RecoveryAction::Cleanup => {
                let call = self.scope.backend(ComputeRequest::Cancel);
                self.call(RecoverStep::StopThenPark, call)
            }
            RecoveryAction::Retire => {
                let ReconcileEvidence::Tombstoned(tombstone) = evidence else {
                    return Ok(Next::Done(false));
                };
                self.record_tombstone(tombstone)
            }
            RecoveryAction::Park => {
                let error = self.error.clone();
                self.park_failed(&error)
            }
        }
    }
}

impl Operation for RecoverSubmitOperation {
    type Output = bool;
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> RecoverNext {
        if self.error == BackendError::Cancelled {
            return self.cancel();
        }
        self.call(RecoverStep::Stops, stops_call())
    }

    fn next(&mut self, result: IoResult) -> RecoverNext {
        let scope = self.scope.clone();
        match std::mem::replace(&mut self.step, RecoverStep::Last) {
            RecoverStep::Stops => match result.event()? {
                Event::Job(JobEvent::Stops(stops)) if stops.cancelled => self.cancel(),
                _ => {
                    let read = read_job_record(scope.job_id);
                    self.call(RecoverStep::Record, IoCall::child(read))
                }
            },
            RecoverStep::Record => match result.child::<Option<JobRecord>, JobMutationError>()? {
                Ok(Some(record)) if record.cancel_requested => self.cancel(),
                Ok(Some(record)) => {
                    self.record = Some(Arc::new(record));
                    let call = scope.backend(ComputeRequest::Reconcile);
                    self.call(RecoverStep::Reconcile, call)
                }
                _ => Ok(Next::Done(false)),
            },
            RecoverStep::Reconcile => match compute_answer(result)? {
                ComputeAnswer::Reconciled(evidence) => self.evidence(evidence),
                _ => {
                    let error = self.error.clone();
                    self.park_failed(&error)
                }
            },
            RecoverStep::Running { finalize } => {
                let Ok(running) = result.child::<JobRecord, JobMutationError>()? else {
                    return Ok(Next::Done(false));
                };
                if running.cancel_requested {
                    return self.cancel();
                }
                match finalize {
                    Some(status) => {
                        let goal = FinalizeGoal::Attempt(Ok(*status));
                        let finalize = FinalizeOperation::new(scope, goal);
                        self.call(RecoverStep::Last, IoCall::child(finalize))
                    }
                    None => Ok(Next::Done(true)),
                }
            }
            RecoverStep::Prepare => match result.child::<PreparedTask, JobError>()? {
                Ok(prepared) => self.submit(prepared),
                Err(error) => self.retry_failed(BackendError::Unavailable(error.message)),
            },
            RecoverStep::Submit => match compute_answer(result)? {
                ComputeAnswer::Submitted(Ok(status)) => self.running(&status, false),
                ComputeAnswer::Submitted(Err(error)) => self.retry_failed(error),
                _ => self.retry_failed(BackendError::Unavailable(
                    "the attempt's backend is gone".to_string(),
                )),
            },
            RecoverStep::Tombstone => match compute_answer(result)? {
                ComputeAnswer::Tombstone(Ok(tombstone)) => self.record_tombstone(tombstone),
                _ => {
                    let error = self.error.clone();
                    self.park_failed(&error)
                }
            },
            RecoverStep::Recorded => {
                if result.child::<AttemptControl, JobMutationError>()?.is_err() {
                    return Ok(Next::Done(false));
                }
                let record = self.record.clone().ok_or(IoError::Unfinished)?;
                let failure = SubmitFailureOperation::new(
                    &record,
                    scope.token,
                    submit_error(&self.error),
                    false,
                    scope.settle(self.now_ms),
                );
                self.call(RecoverStep::Last, IoCall::child(failure))
            }
            RecoverStep::StopThenPark => {
                let _ = compute_answer(result)?;
                let error = self.error.clone();
                self.park_failed(&error)
            }
            RecoverStep::Last => {
                if let Err(error) = result.child::<(), IoError>()? {
                    warn!(job_id = %scope.job_id, %error, "Submit recovery failed to settle");
                }
                Ok(Next::Done(false))
            }
        }
    }
}

#[cfg(test)]
#[path = "recovery_tests.rs"]
mod tests;
