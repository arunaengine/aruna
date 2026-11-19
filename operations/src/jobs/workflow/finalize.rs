//! Finalizes a compute attempt: stores outputs and writes the terminal job state.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::sync::Arc;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::effect::{ComputeAnswer, ComputeRequest};
use aruna_core::compute::session::EndReason;
use aruna_core::compute::{
    AttemptPhase, AttemptStatus, BackendError, CancelEvidence, LogLimits, LogTails,
};
use aruna_core::events::Event;
use aruna_core::jobs::JobEvent;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{
    AttemptControl, JobError, JobErrorKind, JobProgress, JobRecord, JobResultPayload,
    MAX_MESSAGE_BYTES, OutputObject, tail_str,
};
use tracing::{info, warn};

use super::attempt::{AttemptScope, stops_call};
use super::compute::compute_answer;
use super::inventory::CollectOutputsOperation;
use super::outputs::CaptureOutputsOperation;
use super::prepare::job_bucket;
use super::session::SessionReportOperation;
use super::settle::{
    AnnounceTerminalOperation, EndRunOperation, ParkAttemptOperation, RunEnding, execution_result,
};
use super::workspace::merge_outputs;
use crate::jobs::output_record::StoreOutputsOperation;
use crate::jobs::store::job_calls::{
    ExecutionCompleteOutcome, RunSuccess, complete_execution, record_attempt_started,
    transition_to_cancelling,
};
use crate::jobs::store::mutation_error::JobMutationError;
use crate::jobs::store::row_calls::{read_attempt_control, read_job_record};

/// What a run is being finalized for.
#[derive(Debug, PartialEq)]
pub enum FinalizeGoal {
    /// The attempt wait ended with this. Terminalization correlates the durable cancel intent
    /// with the backend exit: Docker cannot tell a SIGKILL 137 from a natural exit.
    Attempt(Result<AttemptStatus, BackendError>),
    /// The job was cancelled.
    Cancel,
    /// A session the client ended, or that went idle: its work is in the workspace bucket.
    Session(EndReason),
    /// The walltime passed.
    Walltime,
}

/// One piece of work left before the terminal write.
#[derive(Debug, PartialEq)]
enum Task {
    /// Exports the file outputs; with `until_stop` a job stop ends the export.
    Export {
        until_stop: bool,
    },
    Control,
    Collect,
    Merge,
    Logs,
    Check,
    Report(EndReason),
    Store,
    Finish(Finish),
}

/// The terminal write that ends the plan.
#[derive(Debug, PartialEq)]
enum Finish {
    /// Success, unless a cancel committed first.
    Execution,
    /// Success of a run whose cancel came too late to stop it.
    AfterCancel,
    Fail(JobError),
    Cancel,
}

#[derive(Debug, PartialEq)]
enum Step {
    /// Reads the job's cancel intent before an attempt result is judged.
    Intent(Box<Result<AttemptStatus, BackendError>>),
    Started(Box<AttemptStatus>),
    Cancelling(StopFor),
    Stop(StopFor),
    ControlFirst(Box<Result<CancelEvidence, BackendError>>),
    ExportRecord {
        until_stop: bool,
    },
    Export {
        until_stop: bool,
    },
    ExportStops(Box<JobError>),
    ExportIntent(Box<JobError>),
    CaptureCancelling,
    Control,
    Collect,
    Logs,
    Report,
    StoreRecord,
    Store(Arc<JobRecord>),
    FailRecord(Box<JobError>),
    Progress(Box<JobResultPayload>),
    Completed,
    /// A child that ends the operation.
    Last,
}

/// Why the backend is told to stop the attempt.
#[derive(Clone, Copy, Debug, PartialEq)]
enum StopFor {
    Cancel,
    Session(EndReason),
    Walltime,
}

type FinalNext = Result<Next<()>, IoError>;

/// Settles an attempt from the evidence of its end: exports and records its outputs when it
/// succeeded, and writes the terminal state. Whatever cannot be decided yet parks the attempt
/// for a later reconcile pass.
#[derive(Debug, PartialEq)]
pub struct FinalizeOperation {
    scope: Arc<AttemptScope>,
    goal: Option<FinalizeGoal>,
    plan: VecDeque<Task>,
    exit_code: Option<i32>,
    captured: Vec<OutputObject>,
    inventoried: Vec<OutputObject>,
    outputs: Vec<OutputObject>,
    logs: LogTails,
    control: Option<AttemptControl>,
    control_failure: Option<JobError>,
    /// The job's cancel intent while its start evidence is written.
    pending_cancel: bool,
    result: Option<JobResultPayload>,
    now_ms: u64,
    step: Step,
}

impl FinalizeOperation {
    pub fn new(scope: Arc<AttemptScope>, goal: FinalizeGoal) -> Self {
        Self {
            scope,
            goal: Some(goal),
            plan: VecDeque::new(),
            exit_code: None,
            captured: Vec::new(),
            inventoried: Vec::new(),
            outputs: Vec::new(),
            logs: LogTails::default(),
            control: None,
            control_failure: None,
            pending_cancel: false,
            result: None,
            now_ms: 0,
            step: Step::Last,
        }
    }

    fn call(&mut self, step: Step, call: IoCall) -> FinalNext {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn park(&mut self, error: JobError) -> FinalNext {
        let scope = &self.scope;
        let settle = scope.settle(self.now_ms);
        let park = ParkAttemptOperation::new(scope.job_id, scope.token, error, settle);
        self.call(Step::Last, IoCall::child(park))
    }

    fn end(&mut self, ending: RunEnding) -> FinalNext {
        let scope = &self.scope;
        let settle = scope.settle(self.now_ms);
        let end = EndRunOperation::new(scope.job_id, scope.token, ending, settle);
        self.call(Step::Last, IoCall::child(end))
    }

    fn result_now(&mut self) -> JobResultPayload {
        execution_result(
            &self.scope.bucket,
            self.exit_code,
            std::mem::take(&mut self.outputs),
            std::mem::take(&mut self.logs),
        )
    }

    /// Terminalizes on a permanent output error so cleanup runs; a job record that cannot be
    /// read parks the attempt instead, leaving the terminal write to a later pass.
    fn fail_or_park(&mut self, error: JobError) -> FinalNext {
        let read = read_job_record(self.scope.job_id);
        self.call(Step::FailRecord(Box::new(error)), IoCall::child(read))
    }

    fn fail_empty(&mut self, exit_code: Option<i32>, error: JobError) -> FinalNext {
        let bucket = &self.scope.bucket;
        let result = execution_result(bucket, exit_code, Vec::new(), LogTails::default());
        self.end(RunEnding::Fail { error, result })
    }

    fn stop_attempt(&mut self, reason: StopFor) -> FinalNext {
        let scope = &self.scope;
        let write = transition_to_cancelling(scope.job_id, scope.token, self.now_ms);
        self.call(Step::Cancelling(reason), IoCall::child(write))
    }

    /// Plans the end of an attempt whose status is known and whose job is not being cancelled.
    fn plan_status(&mut self, status: AttemptStatus) -> FinalNext {
        use Task::{Check, Collect, Control, Export, Finish as End, Logs, Merge, Store};
        match status.phase {
            AttemptPhase::Exited { code: 0 } => {
                self.exit_code = Some(0);
                // Export first: inventory attributes only versions this execution already
                // reserved, so the reservations must be durable before it.
                self.plan = VecDeque::from([
                    Export { until_stop: true },
                    Control,
                    Collect,
                    Merge,
                    Logs,
                    Check,
                    Store,
                    End(Finish::Execution),
                ]);
            }
            AttemptPhase::Exited { code } => {
                self.exit_code = Some(code);
                let detail = status.detail.as_deref();
                warn!(
                    job_id = %self.scope.job_id,
                    code,
                    detail_bytes = detail.map_or(0, str::len),
                    "Container exited non-zero"
                );
                let error = JobError::permanent(exit_message(code, detail));
                self.plan = VecDeque::from([Control, Collect, Logs, End(Finish::Fail(error))]);
            }
            AttemptPhase::Failed { reason } => {
                let error = JobError::permanent(format!("backend failure: {reason}"));
                self.plan = VecDeque::from([Logs, End(Finish::Fail(error))]);
            }
            // Infrastructure evidence proves nothing about the job: park it so the family may
            // still run it elsewhere instead of writing a terminal row.
            AttemptPhase::SystemError { reason } => {
                let message = format!("backend infrastructure failure: {reason}");
                return self.park(JobError::retryable(message));
            }
            AttemptPhase::Cancelled => return self.stop_attempt(StopFor::Cancel),
            AttemptPhase::Submitted | AttemptPhase::Running => {
                return self.park(JobError::retryable("attempt returned non-terminal"));
            }
        }
        self.next_task()
    }

    /// Plans the end of a cancelled attempt from what stopping it showed.
    fn plan_cancelled(&mut self, evidence: Result<CancelEvidence, BackendError>) -> FinalNext {
        use Task::{Check, Collect, Control, Export, Finish as End, Logs, Merge, Store};
        let status = match evidence {
            Ok(CancelEvidence::Stopped(status)) => status,
            Ok(CancelEvidence::AlreadyGone) => {
                self.plan = VecDeque::from([Collect, End(Finish::Cancel)]);
                return self.next_task();
            }
            // No definitive stop evidence yet.
            Ok(CancelEvidence::Requested) | Err(_) => {
                return self.park(JobError::retryable(
                    "cancel requested without stop evidence",
                ));
            }
        };
        self.plan = match status.phase {
            AttemptPhase::Exited { code: 0 } => {
                self.exit_code = Some(0);
                VecDeque::from([
                    Logs,
                    Export { until_stop: false },
                    Control,
                    Collect,
                    Merge,
                    Check,
                    Store,
                    End(Finish::AfterCancel),
                ])
            }
            AttemptPhase::Exited { code } => {
                self.exit_code = Some(code);
                let message = exit_message(code, status.detail.as_deref());
                let error = JobError::permanent(message);
                VecDeque::from([Logs, Collect, End(Finish::Fail(error))])
            }
            AttemptPhase::Failed { reason } => {
                let error = JobError::permanent(format!("backend failure: {reason}"));
                VecDeque::from([Logs, End(Finish::Fail(error))])
            }
            // Infrastructure evidence is no verdict on the job, so a confirmed stop
            // terminalizes as the requested cancellation, never as failed.
            AttemptPhase::SystemError { reason } => {
                warn!(job_id = %self.scope.job_id, %reason, "Cancelled attempt lost backend evidence");
                VecDeque::from([Logs, End(Finish::Cancel)])
            }
            AttemptPhase::Cancelled | AttemptPhase::Submitted | AttemptPhase::Running => {
                VecDeque::from([Logs, Collect, End(Finish::Cancel)])
            }
        };
        self.next_task()
    }

    /// Plans the end of an attempt stopped because its session or its walltime ended.
    fn plan_stopped(
        &mut self,
        reason: StopFor,
        evidence: Result<CancelEvidence, BackendError>,
    ) -> FinalNext {
        use Task::{Control, Finish as End, Logs, Report, Store};
        let lacking = match reason {
            StopFor::Walltime => "walltime stop lacks evidence",
            _ => "session stop lacks evidence",
        };
        let mut plan = match evidence {
            Ok(CancelEvidence::Stopped(_)) => VecDeque::from([Logs]),
            Ok(CancelEvidence::AlreadyGone) => VecDeque::new(),
            Ok(CancelEvidence::Requested) | Err(_) => {
                return self.park(JobError::retryable(lacking));
            }
        };
        match reason {
            StopFor::Walltime => {
                let error = JobError::permanent("walltime limit exceeded");
                plan.push_back(End(Finish::Fail(error)));
            }
            // A kernel that died is a permanent execution failure, not a session the caller
            // finished; any other end needs a named output record, even with no outputs.
            StopFor::Session(EndReason::KernelExit) => {
                self.exit_code = Some(1);
                plan.push_back(Report(EndReason::KernelExit));
                let error = JobError::permanent("session kernel exited");
                plan.push_back(End(Finish::Fail(error)));
            }
            StopFor::Session(ended) => {
                self.exit_code = Some(0);
                info!(job_id = %self.scope.job_id, reason = ended.as_str(), "Session finished");
                plan.extend([Report(ended), Control, Store, End(Finish::AfterCancel)]);
            }
            StopFor::Cancel => {}
        }
        self.plan = plan;
        self.next_task()
    }

    fn next_task(&mut self) -> FinalNext {
        let Some(task) = self.plan.pop_front() else {
            return Ok(Next::Done(()));
        };
        let scope = self.scope.clone();
        match task {
            Task::Export { until_stop } => {
                if scope.spec.file_outputs.is_empty() {
                    return self.next_task();
                }
                let read = read_job_record(scope.job_id);
                self.call(Step::ExportRecord { until_stop }, IoCall::child(read))
            }
            Task::Control => {
                let epoch = scope.fence.attempt_epoch;
                let read = read_attempt_control(scope.job_id, epoch);
                self.call(Step::Control, IoCall::child(read))
            }
            Task::Collect => {
                let node = scope.wiring.serving.map(|(node, _)| node);
                let control = self.control.clone().ok_or(IoError::Unfinished)?;
                let collect =
                    CollectOutputsOperation::new(&scope.spec, &scope.bucket, control, node);
                self.call(Step::Collect, IoCall::child(collect))
            }
            Task::Merge => {
                let inventoried = std::mem::take(&mut self.inventoried);
                let captured = std::mem::take(&mut self.captured);
                match merge_outputs(inventoried, captured) {
                    Ok(outputs) => {
                        self.outputs = outputs;
                        self.next_task()
                    }
                    Err(error) => {
                        warn!(job_id = %scope.job_id, bucket = %scope.bucket, error = ?error, "Output manifest merge failed permanently; failing");
                        self.fail_or_park(error)
                    }
                }
            }
            Task::Logs => {
                let defaults = LogLimits::default();
                let limits = LogLimits {
                    max_stream_bytes: defaults.inline_tail_bytes,
                    ..defaults
                };
                self.call(Step::Logs, scope.backend(ComputeRequest::FetchLogs(limits)))
            }
            Task::Check => {
                let result = self.result_now();
                let control = self.control.as_ref().ok_or(IoError::Unfinished)?;
                match result.check_outputs(control.execution_id) {
                    Ok(()) => {
                        self.result = Some(result);
                        self.next_task()
                    }
                    // Success is refused when an output cannot be named exactly.
                    Err(error) => {
                        warn!(job_id = %scope.job_id, error = %error, "Output identity incomplete; failing");
                        let message = format!("output identity incomplete: {error}");
                        self.fail_empty(None, JobError::permanent(message))
                    }
                }
            }
            Task::Report(reason) => {
                let report = SessionReportOperation::new(scope.job_id, scope.token, true, reason);
                self.call(Step::Report, IoCall::child(report))
            }
            Task::Store => {
                if self.result.is_none() {
                    self.result = Some(self.result_now());
                }
                let read = read_job_record(scope.job_id);
                self.call(Step::StoreRecord, IoCall::child(read))
            }
            Task::Finish(finish) => self.finish(finish),
        }
    }

    fn finish(&mut self, finish: Finish) -> FinalNext {
        let result = match self.result.take() {
            Some(result) => result,
            None => self.result_now(),
        };
        match finish {
            Finish::Fail(error) => self.end(RunEnding::Fail { error, result }),
            Finish::Cancel => self.end(RunEnding::Cancel { result }),
            Finish::AfterCancel => self.end(RunEnding::Complete { result }),
            Finish::Execution => {
                let read = read_job_record(self.scope.job_id);
                self.call(Step::Progress(Box::new(result)), IoCall::child(read))
            }
        }
    }

    /// A cancelled run settles without its outputs; cleanup removes its capture helpers.
    fn cancel_capture(&mut self) -> FinalNext {
        let scope = &self.scope;
        let write = transition_to_cancelling(scope.job_id, scope.token, self.now_ms);
        self.call(Step::CaptureCancelling, IoCall::child(write))
    }

    /// A failed capture of a cancelled run settles it as cancelled instead of retrying it.
    fn capture_failed(&mut self, error: JobError) -> FinalNext {
        if error.kind == JobErrorKind::Retryable {
            warn!(job_id = %self.scope.job_id, error = ?error, "Output capture failed; parking");
            return self.park(error);
        }
        self.fail_empty(Some(0), error)
    }

    fn intent_read(
        &mut self,
        cancel_requested: bool,
        result: Result<AttemptStatus, BackendError>,
    ) -> FinalNext {
        let scope = self.scope.clone();
        let status = match result {
            Ok(status) => status,
            // A missing attempt or an unreachable backend after submit is ambiguous: park it,
            // never requeue.
            Err(error) => {
                let message = format!("attempt unobservable: {error}");
                return self.park(JobError::retryable(message));
            }
        };
        if let Some(started_at_ms) = status.started_at_ms {
            let write = record_attempt_started(scope.job_id, scope.token, started_at_ms);
            let status = Box::new(status);
            self.exit_code = None;
            self.pending_cancel = cancel_requested;
            return self.call(Step::Started(status), IoCall::child(write));
        }
        self.judge(cancel_requested, status)
    }

    fn judge(&mut self, cancel_requested: bool, status: AttemptStatus) -> FinalNext {
        match cancel_requested {
            true => self.stop_attempt(StopFor::Cancel),
            false => self.plan_status(status),
        }
    }
}

impl Operation for FinalizeOperation {
    type Output = ();
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> FinalNext {
        let scope = self.scope.clone();
        match self.goal.take() {
            Some(FinalizeGoal::Attempt(result)) => {
                let read = read_job_record(scope.job_id);
                self.call(Step::Intent(Box::new(result)), IoCall::child(read))
            }
            Some(FinalizeGoal::Cancel) | None => self.stop_attempt(StopFor::Cancel),
            Some(FinalizeGoal::Session(reason)) => self.stop_attempt(StopFor::Session(reason)),
            Some(FinalizeGoal::Walltime) => self.stop_attempt(StopFor::Walltime),
        }
    }

    fn next(&mut self, result: IoResult) -> FinalNext {
        let scope = self.scope.clone();
        match std::mem::replace(&mut self.step, Step::Last) {
            Step::Intent(attempt) => {
                let record = result.child::<Option<JobRecord>, JobMutationError>()?;
                let cancel_requested = record
                    .ok()
                    .flatten()
                    .is_some_and(|record| record.cancel_requested);
                self.intent_read(cancel_requested, *attempt)
            }
            Step::Started(status) => match result.child::<JobRecord, JobMutationError>()? {
                Ok(_) => self.judge(self.pending_cancel, *status),
                Err(error) => {
                    warn!(job_id = %scope.job_id, error = %error, "Attempt start evidence write failed");
                    Ok(Next::Done(()))
                }
            },
            // A re-entry may already be cancelling, so this write may lose harmlessly.
            Step::Cancelling(reason) => {
                let _ = result.child::<JobRecord, JobMutationError>()?;
                self.call(Step::Stop(reason), scope.backend(ComputeRequest::Cancel))
            }
            Step::Stop(reason) => {
                let evidence = match compute_answer(result)? {
                    ComputeAnswer::Cancelled(evidence) => evidence,
                    _ => Err(BackendError::Unavailable("no backend".to_string())),
                };
                match reason {
                    // The identity is only needed to record outputs, so the stop comes first.
                    StopFor::Cancel => {
                        let epoch = scope.fence.attempt_epoch;
                        let read = read_attempt_control(scope.job_id, epoch);
                        let step = Step::ControlFirst(Box::new(evidence));
                        self.call(step, IoCall::child(read))
                    }
                    other => self.plan_stopped(other, evidence),
                }
            }
            Step::ControlFirst(evidence) => {
                self.control_read(result)?;
                if self.control.is_none() {
                    return self.control_failed();
                }
                self.plan_cancelled(*evidence)
            }
            Step::ExportRecord { until_stop } => {
                match result.child::<Option<JobRecord>, JobMutationError>()? {
                    Ok(Some(record)) => {
                        let capture = scope.capture(&Arc::new(record), until_stop);
                        let Some(capture) = capture else {
                            let error = JobError::permanent("output capture needs a net handle");
                            return self.fail_empty(Some(0), error);
                        };
                        let capture = CaptureOutputsOperation::new(capture);
                        let step = Step::Export { until_stop };
                        self.call(step, IoCall::child(capture))
                    }
                    Ok(None) => Ok(Next::Done(())),
                    Err(error) => {
                        let message = format!("output job lookup failed: {error}");
                        self.park(JobError::retryable(message))
                    }
                }
            }
            Step::Export { until_stop } => {
                match result.child::<Vec<OutputObject>, JobError>()? {
                    Ok(outputs) => {
                        self.captured = outputs;
                        self.next_task()
                    }
                    // The cancel path itself captures what finished before the cancel.
                    Err(error) if !until_stop => {
                        info!(job_id = %scope.job_id, error = ?error, "Output capture of a cancelled run stopped");
                        self.cancel_capture()
                    }
                    Err(error) => self.call(Step::ExportStops(Box::new(error)), stops_call()),
                }
            }
            Step::ExportStops(error) => match result.event()? {
                Event::Job(JobEvent::Stops(stops)) if stops.cancelled => {
                    info!(job_id = %scope.job_id, error = ?error, "Output capture of a cancelled run stopped");
                    self.cancel_capture()
                }
                _ => {
                    let read = read_job_record(scope.job_id);
                    self.call(Step::ExportIntent(error), IoCall::child(read))
                }
            },
            Step::ExportIntent(error) => {
                let record = result.child::<Option<JobRecord>, JobMutationError>()?;
                match record
                    .ok()
                    .flatten()
                    .is_some_and(|record| record.cancel_requested)
                {
                    true => {
                        info!(job_id = %scope.job_id, error = ?error, "Output capture of a cancelled run stopped");
                        self.cancel_capture()
                    }
                    false => self.capture_failed(*error),
                }
            }
            Step::CaptureCancelling => {
                let _ = result.child::<JobRecord, JobMutationError>()?;
                let bucket = &scope.bucket;
                let result = execution_result(bucket, Some(0), Vec::new(), LogTails::default());
                self.end(RunEnding::Cancel { result })
            }
            Step::Control => {
                self.control_read(result)?;
                if self.control.is_none() {
                    return self.control_failed();
                }
                self.next_task()
            }
            Step::Collect => match result.child::<Vec<OutputObject>, JobError>()? {
                Ok(outputs) => {
                    // A plan with a merge keeps both lists; any other keeps the inventory.
                    match self.plan.front() {
                        Some(Task::Merge) => self.inventoried = outputs,
                        _ => self.outputs = outputs,
                    }
                    self.next_task()
                }
                Err(error) if error.kind == JobErrorKind::Permanent => {
                    warn!(job_id = %scope.job_id, bucket = %scope.bucket, error = ?error, "Output inventory failed permanently; failing");
                    self.fail_or_park(error)
                }
                Err(error) => {
                    warn!(job_id = %scope.job_id, bucket = %scope.bucket, error = ?error, "Output inventory failed; parking");
                    self.park(error)
                }
            },
            Step::Logs => match compute_answer(result)? {
                ComputeAnswer::Logs(Ok(logs)) => {
                    self.logs = logs;
                    self.next_task()
                }
                ComputeAnswer::Logs(Err(error)) => {
                    warn!(job_id = %scope.job_id, error = %error, "Container log capture failed");
                    let message = format!("container log capture failed: {error}");
                    self.park(JobError::retryable(message))
                }
                _ => self.park(JobError::retryable(
                    "container log capture failed: backend unavailable",
                )),
            },
            Step::Report => {
                let _ = result.child::<(), IoError>()?;
                self.next_task()
            }
            Step::StoreRecord => match result.child::<Option<JobRecord>, JobMutationError>()? {
                Ok(Some(record)) => {
                    let control = self.control.as_ref().ok_or(IoError::Unfinished)?;
                    let Some(JobResultPayload::Execution { outputs, .. }) = &self.result else {
                        return Ok(Next::Done(()));
                    };
                    let store = StoreOutputsOperation::new(
                        &record,
                        control,
                        outputs,
                        scope.wiring.serving,
                        self.now_ms,
                    );
                    self.call(Step::Store(Arc::new(record)), IoCall::child(store))
                }
                _ => self.park(JobError::retryable("output record job lookup failed")),
            },
            // A success whose immutable output record is not durable must never be published.
            Step::Store(record) => match result.child::<[u8; 32], JobError>()? {
                Ok(digest) => {
                    if let Some(result) = self.result.as_mut() {
                        name_output_record(result, digest);
                    }
                    self.next_task()
                }
                Err(error) if error.kind == JobErrorKind::Permanent => {
                    warn!(job_id = %scope.job_id, bucket = %scope.bucket, error = ?error, "Output record store failed; failing");
                    let bucket = job_bucket(&record);
                    let result = execution_result(&bucket, None, Vec::new(), LogTails::default());
                    self.end(RunEnding::Fail { error, result })
                }
                Err(error) => self.park(error),
            },
            Step::FailRecord(error) => match result
                .child::<Option<JobRecord>, JobMutationError>()?
            {
                Ok(Some(record)) => {
                    let bucket = job_bucket(&record);
                    let result = execution_result(&bucket, None, Vec::new(), LogTails::default());
                    self.end(RunEnding::Fail {
                        error: *error,
                        result,
                    })
                }
                _ => self.park(*error),
            },
            Step::Progress(payload) => {
                let record = result.child::<Option<JobRecord>, JobMutationError>()?;
                let progress = record
                    .ok()
                    .flatten()
                    .map_or_else(|| JobProgress::new("phases"), |record| record.progress);
                let success = RunSuccess {
                    result: *payload,
                    progress,
                    now_ms: self.now_ms,
                };
                let write = complete_execution(scope.job_id, scope.token, success);
                self.call(Step::Completed, IoCall::child(write))
            }
            Step::Completed => {
                match result.child::<ExecutionCompleteOutcome, JobMutationError>()? {
                    Ok(ExecutionCompleteOutcome::Completed(record)) => {
                        let settle = scope.settle(self.now_ms);
                        let announce = AnnounceTerminalOperation::new(record, settle);
                        self.call(Step::Last, IoCall::child(announce))
                    }
                    Ok(ExecutionCompleteOutcome::CancelRequested(_)) => {
                        *self = Self::new(scope, FinalizeGoal::Cancel).with_clock(self.now_ms);
                        self.stop_attempt(StopFor::Cancel)
                    }
                    Err(error) => {
                        warn!(job_id = %scope.job_id, error = %error, "Execution complete write failed");
                        Ok(Next::Done(()))
                    }
                }
            }
            Step::Last => {
                if let Err(error) = result.child::<(), IoError>()? {
                    warn!(job_id = %scope.job_id, %error, "Attempt settlement failed");
                }
                Ok(Next::Done(()))
            }
        }
    }
}

impl FinalizeOperation {
    fn with_clock(mut self, now_ms: u64) -> Self {
        self.now_ms = now_ms;
        self
    }

    /// Keeps the physical execution identity of the fenced attempt, when it is readable.
    fn control_read(&mut self, result: IoResult) -> Result<(), IoError> {
        let read = result.child::<Option<AttemptControl>, JobMutationError>()?;
        self.control_failure = None;
        self.control = match read {
            Ok(Some(control)) if !control.execution_id.is_nil() => Some(control),
            Ok(_) => {
                let error = "attempt control carries no execution identity";
                self.control_failure = Some(JobError::retryable(error));
                None
            }
            Err(error) => {
                let message = format!("attempt control lookup failed: {error}");
                self.control_failure = Some(JobError::retryable(message));
                None
            }
        };
        Ok(())
    }

    /// Without the identity no output can be named exactly, so the attempt is parked.
    fn control_failed(&mut self) -> FinalNext {
        let error = self
            .control_failure
            .take()
            .unwrap_or_else(|| JobError::retryable("attempt control lookup failed"));
        self.park(error)
    }
}

/// Name the durable output record on a terminal success result.
fn name_output_record(result: &mut JobResultPayload, digest: [u8; 32]) {
    if let JobResultPayload::Execution { output_digest, .. } = result {
        *output_digest = Some(digest);
    }
}

/// Failure message for a non-zero exit, keeping the exit code readable when the
/// backend detail would not fit the stored message.
pub fn exit_message(code: i32, detail: Option<&str>) -> String {
    let head = format!("container exited with code {code}");
    let Some(detail) = detail.map(str::trim).filter(|text| !text.is_empty()) else {
        return head;
    };
    let room = MAX_MESSAGE_BYTES.saturating_sub(head.len() + 2);
    format!("{head}: {}", tail_str(detail, room))
}

#[cfg(test)]
#[path = "finalize_tests.rs"]
mod call_tests;
