//! Tests how an attempt is settled, by the calls and results of its finalization.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::execution::job::{OutputDestination, OutputSelection};
use ulid::Ulid;

use super::super::fixtures::{
    BUCKET, EPOCH, child, compute, control, is_child, job, node, record, scope, scope_of, spec,
    status, stops, token,
};
use super::*;

const NOW: u64 = 9;

fn finalize(goal: FinalizeGoal) -> FinalizeOperation {
    let mut finalize = FinalizeOperation::new(scope(), goal);
    finalize.stamp(NOW);
    finalize
}

fn exited(code: i32) -> AttemptStatus {
    status(AttemptPhase::Exited { code })
}

fn job_read(job: JobRecord) -> IoResult {
    child(Ok::<_, JobMutationError>(Some(job)))
}

fn stored() -> IoResult {
    child(Ok::<_, JobMutationError>(record()))
}

fn control_read() -> IoResult {
    child(Ok::<_, JobMutationError>(Some(control())))
}

fn outputs_found() -> IoResult {
    child(Ok::<Vec<OutputObject>, JobError>(Vec::new()))
}

fn logs_read() -> IoResult {
    compute(ComputeAnswer::Logs(Ok(LogTails::default())))
}

fn settled() -> IoResult {
    child(Ok::<(), IoError>(()))
}

fn stop_answer(evidence: Result<CancelEvidence, BackendError>) -> IoResult {
    compute(ComputeAnswer::Cancelled(evidence))
}

fn read_job() -> Next<()> {
    Next::Call(IoCall::child(read_job_record(job())))
}

fn read_control() -> Next<()> {
    Next::Call(IoCall::child(read_attempt_control(job(), EPOCH)))
}

fn collect() -> Next<()> {
    let scope = scope();
    let operation =
        CollectOutputsOperation::new(&scope.spec, &scope.bucket, control(), Some(node()));
    Next::Call(IoCall::child(operation))
}

fn fetch_logs() -> Next<()> {
    let defaults = LogLimits::default();
    let limits = LogLimits {
        max_stream_bytes: defaults.inline_tail_bytes,
        ..defaults
    };
    Next::Call(scope().backend(ComputeRequest::FetchLogs(limits)))
}

fn cancelling() -> Next<()> {
    let write = transition_to_cancelling(job(), token(), NOW);
    Next::Call(IoCall::child(write))
}

fn stop_backend() -> Next<()> {
    Next::Call(scope().backend(ComputeRequest::Cancel))
}

fn parked(error: JobError) -> Next<()> {
    let park = ParkAttemptOperation::new(job(), token(), error, scope().settle(NOW));
    Next::Call(IoCall::child(park))
}

fn ended(ending: RunEnding) -> Next<()> {
    let end = EndRunOperation::new(job(), token(), ending, scope().settle(NOW));
    Next::Call(IoCall::child(end))
}

fn result(exit_code: Option<i32>) -> JobResultPayload {
    execution_result(BUCKET, exit_code, Vec::new(), LogTails::default())
}

fn failed(message: &str, exit_code: Option<i32>) -> Next<()> {
    ended(RunEnding::Fail {
        error: JobError::permanent(message),
        result: result(exit_code),
    })
}

fn digested(exit_code: Option<i32>) -> JobResultPayload {
    let mut result = result(exit_code);
    name_output_record(&mut result, [5; 32]);
    result
}

/// A finalization of the attempt status, past reading the job's cancel intent.
fn judged(attempt: Result<AttemptStatus, BackendError>) -> (FinalizeOperation, Next<()>) {
    let mut finalize = finalize(FinalizeGoal::Attempt(attempt));
    assert_eq!(finalize.first(), Ok(read_job()));
    let next = finalize.next(job_read(record())).unwrap();
    (finalize, next)
}

/// A finalization for `goal` that has just told the backend to stop.
fn stopping(goal: FinalizeGoal) -> FinalizeOperation {
    let mut finalize = finalize(goal);
    assert_eq!(finalize.first(), Ok(cancelling()));
    assert_eq!(finalize.next(stored()), Ok(stop_backend()));
    finalize
}

// A clean exit walks the plan and publishes success naming the stored output record.
#[test]
fn clean_exit_completes() {
    let (mut finalize, next) = judged(Ok(exited(0)));
    assert_eq!(next, read_control());
    assert_eq!(finalize.next(control_read()), Ok(collect()));
    assert_eq!(finalize.next(outputs_found()), Ok(fetch_logs()));
    assert_eq!(finalize.next(logs_read()), Ok(read_job()));
    assert!(is_child(&finalize.next(job_read(record()))));
    let digest = child(Ok::<_, JobError>([5; 32]));
    assert_eq!(finalize.next(digest), Ok(read_job()));
    assert_eq!(finalize.step, Step::Progress(Box::new(digested(Some(0)))));
    let success = RunSuccess {
        result: digested(Some(0)),
        progress: record().progress,
        now_ms: NOW,
    };
    let write = complete_execution(job(), token(), success);
    assert_eq!(
        finalize.next(job_read(record())),
        Ok(Next::Call(IoCall::child(write)))
    );
    let announce = AnnounceTerminalOperation::new(record(), scope().settle(NOW));
    let completed = ExecutionCompleteOutcome::Completed(record());
    assert_eq!(
        finalize.next(child(Ok::<_, JobMutationError>(completed))),
        Ok(Next::Call(IoCall::child(announce)))
    );
    assert_eq!(finalize.next(settled()), Ok(Next::Done(())));
}

// A cancel that committed before the success write turns the run into a cancel.
#[test]
fn late_cancel_restarts() {
    let mut finalize = finalize(FinalizeGoal::Walltime);
    finalize.step = Step::Completed;
    let raced = ExecutionCompleteOutcome::CancelRequested(record());
    assert_eq!(
        finalize.next(child(Ok::<_, JobMutationError>(raced))),
        Ok(cancelling())
    );
    assert_eq!(finalize.next(stored()), Ok(stop_backend()));
}

// A cancel intent outranks the exit the backend reported.
#[test]
fn cancel_intent_stops() {
    let mut finalize = finalize(FinalizeGoal::Attempt(Ok(exited(0))));
    finalize.first().unwrap();
    let mut job = record();
    job.cancel_requested = true;
    assert_eq!(finalize.next(job_read(job)), Ok(cancelling()));
}

// Start evidence is stored first, and the intent read before it still decides.
#[test]
fn start_evidence_written() {
    let mut started = exited(0);
    started.started_at_ms = Some(5);
    let mut finalize = finalize(FinalizeGoal::Attempt(Ok(started)));
    finalize.first().unwrap();
    let mut job = record();
    job.cancel_requested = true;
    let write = record_attempt_started(record().job_id, token(), 5);
    assert_eq!(
        finalize.next(job_read(job)),
        Ok(Next::Call(IoCall::child(write)))
    );
    assert_eq!(finalize.next(stored()), Ok(cancelling()));
}

// A lost start write means another owner holds the attempt.
#[test]
fn lost_start_ends() {
    let mut started = exited(0);
    started.started_at_ms = Some(5);
    let (mut finalize, _) = judged(Ok(started));
    let lost = child(Err::<JobRecord, _>(JobMutationError::TokenMismatch));
    assert_eq!(finalize.next(lost), Ok(Next::Done(())));
}

// Evidence that says nothing about the job parks it instead of writing a terminal state.
#[test]
fn unclear_status_parks() {
    let lost = BackendError::Unavailable("gone".to_string());
    let cases = [
        (Err(lost.clone()), format!("attempt unobservable: {lost}")),
        (
            Ok(status(AttemptPhase::SystemError {
                reason: "disk".to_string(),
            })),
            "backend infrastructure failure: disk".to_string(),
        ),
        (
            Ok(status(AttemptPhase::Running)),
            "attempt returned non-terminal".to_string(),
        ),
    ];
    for (attempt, message) in cases {
        let (_, next) = judged(attempt);
        assert_eq!(next, parked(JobError::retryable(message)));
    }
}

// A non-zero exit keeps its inventory and logs, then fails with the code.
#[test]
fn failed_exit_fails() {
    let mut ended_badly = exited(3);
    ended_badly.detail = Some("boom".to_string());
    let (mut finalize, next) = judged(Ok(ended_badly));
    assert_eq!(next, read_control());
    assert_eq!(finalize.next(control_read()), Ok(collect()));
    assert_eq!(finalize.next(outputs_found()), Ok(fetch_logs()));
    assert_eq!(
        finalize.next(logs_read()),
        Ok(failed("container exited with code 3: boom", Some(3)))
    );
}

// A backend failure keeps only the logs.
#[test]
fn backend_failure_fails() {
    let phase = AttemptPhase::Failed {
        reason: "oom".to_string(),
    };
    let (mut finalize, next) = judged(Ok(status(phase)));
    assert_eq!(next, fetch_logs());
    assert_eq!(
        finalize.next(logs_read()),
        Ok(failed("backend failure: oom", None))
    );
}

// Logs that cannot be read park the attempt, so the evidence is not lost.
#[test]
fn unread_logs_park() {
    let phase = AttemptPhase::Failed {
        reason: "oom".to_string(),
    };
    let (mut finalize, _) = judged(Ok(status(phase)));
    let message = "container log capture failed: backend unavailable";
    assert_eq!(
        finalize.next(compute(ComputeAnswer::NoBackend)),
        Ok(parked(JobError::retryable(message)))
    );
}

// An attempt whose container is gone settles as cancelled with what it left.
#[test]
fn stopped_gone_cancels() {
    let mut finalize = stopping(FinalizeGoal::Cancel);
    let gone = stop_answer(Ok(CancelEvidence::AlreadyGone));
    assert_eq!(finalize.next(gone), Ok(read_control()));
    assert_eq!(finalize.next(control_read()), Ok(collect()));
    assert_eq!(
        finalize.next(outputs_found()),
        Ok(ended(RunEnding::Cancel {
            result: result(None)
        }))
    );
}

// A stop without proof parks the attempt, whatever the answer was.
#[test]
fn unproved_stop_parks() {
    let answers = [
        stop_answer(Ok(CancelEvidence::Requested)),
        stop_answer(Err(BackendError::Timeout("slow".to_string()))),
        compute(ComputeAnswer::NoBackend),
    ];
    for answer in answers {
        let mut finalize = stopping(FinalizeGoal::Cancel);
        finalize.next(answer).unwrap();
        assert_eq!(
            finalize.next(control_read()),
            Ok(parked(JobError::retryable(
                "cancel requested without stop evidence"
            )))
        );
    }
}

// A run that finished before the cancel reached it still completes.
#[test]
fn late_exit_completes() {
    let mut finalize = stopping(FinalizeGoal::Cancel);
    let stopped = stop_answer(Ok(CancelEvidence::Stopped(exited(0))));
    finalize.next(stopped).unwrap();
    assert_eq!(finalize.next(control_read()), Ok(fetch_logs()));
    assert_eq!(finalize.next(logs_read()), Ok(read_control()));
    assert_eq!(finalize.next(control_read()), Ok(collect()));
    assert_eq!(finalize.next(outputs_found()), Ok(read_job()));
    assert!(is_child(&finalize.next(job_read(record()))));
    let digest = child(Ok::<_, JobError>([5; 32]));
    assert_eq!(
        finalize.next(digest),
        Ok(ended(RunEnding::Complete {
            result: digested(Some(0)),
        }))
    );
}

// Lost infrastructure evidence is no verdict: a confirmed stop stays a cancel.
#[test]
fn system_error_cancels() {
    let phase = AttemptPhase::SystemError {
        reason: "disk".to_string(),
    };
    let mut finalize = stopping(FinalizeGoal::Cancel);
    let stopped = stop_answer(Ok(CancelEvidence::Stopped(status(phase))));
    finalize.next(stopped).unwrap();
    assert_eq!(finalize.next(control_read()), Ok(fetch_logs()));
    assert_eq!(
        finalize.next(logs_read()),
        Ok(ended(RunEnding::Cancel {
            result: result(None)
        }))
    );
}

// Without the execution identity no output can be named, so the attempt parks.
#[test]
fn missing_control_parks() {
    let mut unnamed = control();
    unnamed.execution_id = Ulid::nil();
    let identity = "attempt control carries no execution identity";
    let lookup = format!(
        "attempt control lookup failed: {}",
        JobMutationError::NotFound
    );
    let cases = [
        (
            child(Ok::<Option<AttemptControl>, JobMutationError>(None)),
            identity.to_string(),
        ),
        (
            child(Ok::<_, JobMutationError>(Some(unnamed))),
            identity.to_string(),
        ),
        (
            child(Err::<Option<AttemptControl>, _>(JobMutationError::NotFound)),
            lookup,
        ),
    ];
    for (read, message) in cases {
        let (mut finalize, _) = judged(Ok(exited(0)));
        assert_eq!(
            finalize.next(read),
            Ok(parked(JobError::retryable(message)))
        );
    }
}

// A walltime stop fails the job, with logs only when the container was still there.
#[test]
fn walltime_stop_fails() {
    let message = "walltime limit exceeded";
    let mut stopped = stopping(FinalizeGoal::Walltime);
    let answer = stop_answer(Ok(CancelEvidence::Stopped(status(AttemptPhase::Cancelled))));
    assert_eq!(stopped.next(answer), Ok(fetch_logs()));
    assert_eq!(stopped.next(logs_read()), Ok(failed(message, None)));
    let mut gone = stopping(FinalizeGoal::Walltime);
    let answer = stop_answer(Ok(CancelEvidence::AlreadyGone));
    assert_eq!(gone.next(answer), Ok(failed(message, None)));
    let mut unproved = stopping(FinalizeGoal::Walltime);
    let answer = stop_answer(Ok(CancelEvidence::Requested));
    assert_eq!(
        unproved.next(answer),
        Ok(parked(JobError::retryable("walltime stop lacks evidence")))
    );
}

// A session the client ended counts as a finished run with a named output record.
#[test]
fn ended_session_completes() {
    let mut finalize = stopping(FinalizeGoal::Session(EndReason::Idle));
    let report = SessionReportOperation::new(job(), token(), true, EndReason::Idle);
    let gone = stop_answer(Ok(CancelEvidence::AlreadyGone));
    assert_eq!(finalize.next(gone), Ok(Next::Call(IoCall::child(report))));
    assert_eq!(finalize.next(settled()), Ok(read_control()));
    assert_eq!(finalize.next(control_read()), Ok(read_job()));
    assert!(is_child(&finalize.next(job_read(record()))));
    let digest = child(Ok::<_, JobError>([5; 32]));
    assert_eq!(
        finalize.next(digest),
        Ok(ended(RunEnding::Complete {
            result: digested(Some(0)),
        }))
    );
}

// A kernel that died is a failed run, reported before the failure is written.
#[test]
fn kernel_exit_fails() {
    let mut finalize = stopping(FinalizeGoal::Session(EndReason::KernelExit));
    let gone = stop_answer(Ok(CancelEvidence::AlreadyGone));
    assert!(is_child(&finalize.next(gone)));
    assert_eq!(
        finalize.next(settled()),
        Ok(failed("session kernel exited", Some(1)))
    );
}

fn with_output() -> Arc<AttemptScope> {
    let mut spec = spec();
    spec.file_outputs.push(OutputSelection {
        container_path: "/out/a.txt".to_string(),
        path_prefix: None,
        destination_node_id: Some(node()),
        destination: OutputDestination::S3 {
            bucket: "out".to_string(),
            key: "a.txt".to_string(),
        },
        name: None,
        description: None,
    });
    scope_of(spec)
}

/// A clean exit whose output export just reported `error`.
fn export_failed(error: JobError) -> (FinalizeOperation, Result<Next<()>, IoError>) {
    let mut finalize = FinalizeOperation::new(with_output(), FinalizeGoal::Walltime);
    finalize.stamp(NOW);
    finalize.exit_code = Some(0);
    finalize.step = Step::Export { until_stop: true };
    let next = finalize.next(child(Err::<Vec<OutputObject>, _>(error)));
    (finalize, next)
}

fn end_of_output(ending: RunEnding) -> Next<()> {
    let scope = with_output();
    let end = EndRunOperation::new(job(), token(), ending, scope.settle(NOW));
    Next::Call(IoCall::child(end))
}

// An export that a cancel ended settles the run as cancelled, without outputs.
#[test]
fn stopped_export_cancels() {
    let (mut finalize, next) = export_failed(JobError::retryable("cut"));
    assert_eq!(next, Ok(Next::Call(stops_call())));
    assert_eq!(finalize.next(stops(true)), Ok(cancelling()));
    assert_eq!(
        finalize.next(stored()),
        Ok(end_of_output(RunEnding::Cancel {
            result: result(Some(0)),
        }))
    );
}

// Without a cancel a retryable export error parks, and a permanent one fails the job.
#[test]
fn failed_export_judged() {
    let (mut retried, _) = export_failed(JobError::retryable("cut"));
    assert_eq!(retried.next(stops(false)), Ok(read_job()));
    let park = ParkAttemptOperation::new(
        job(),
        token(),
        JobError::retryable("cut"),
        with_output().settle(NOW),
    );
    assert_eq!(
        retried.next(job_read(record())),
        Ok(Next::Call(IoCall::child(park)))
    );
    let (mut refused, _) = export_failed(JobError::permanent("denied"));
    refused.next(stops(false)).unwrap();
    assert_eq!(
        refused.next(job_read(record())),
        Ok(end_of_output(RunEnding::Fail {
            error: JobError::permanent("denied"),
            result: result(Some(0)),
        }))
    );
}

// A cancel intent stored meanwhile also ends a failed export as cancelled.
#[test]
fn export_sees_intent() {
    let (mut finalize, _) = export_failed(JobError::permanent("denied"));
    finalize.next(stops(false)).unwrap();
    let mut job = record();
    job.cancel_requested = true;
    assert_eq!(finalize.next(job_read(job)), Ok(cancelling()));
}

// The cancel path's own export ends quietly when it fails.
#[test]
fn cancel_export_stops() {
    let mut finalize = FinalizeOperation::new(with_output(), FinalizeGoal::Cancel);
    finalize.stamp(NOW);
    finalize.step = Step::Export { until_stop: false };
    let failed = child(Err::<Vec<OutputObject>, _>(JobError::permanent("denied")));
    assert_eq!(finalize.next(failed), Ok(cancelling()));
}

/// A clean exit at its output inventory.
fn collecting() -> FinalizeOperation {
    let (mut finalize, _) = judged(Ok(exited(0)));
    finalize.next(control_read()).unwrap();
    finalize
}

// A retryable inventory failure parks; a permanent one fails under the job's bucket.
#[test]
fn failed_inventory_judged() {
    let retry = JobError::retryable("slow");
    let unread = child(Err::<Vec<OutputObject>, _>(retry.clone()));
    assert_eq!(collecting().next(unread), Ok(parked(retry)));
    let mut finalize = collecting();
    let error = JobError::permanent("bad");
    let refused = child(Err::<Vec<OutputObject>, _>(error.clone()));
    assert_eq!(finalize.next(refused), Ok(read_job()));
    let empty = execution_result("", None, Vec::new(), LogTails::default());
    assert_eq!(
        finalize.next(job_read(record())),
        Ok(ended(RunEnding::Fail {
            error: error.clone(),
            result: empty,
        }))
    );
    let mut unread = collecting();
    unread
        .next(child(Err::<Vec<OutputObject>, _>(error.clone())))
        .unwrap();
    let missing = child(Ok::<Option<JobRecord>, JobMutationError>(None));
    assert_eq!(unread.next(missing), Ok(parked(error)));
}

// A success whose record is not durable never publishes.
#[test]
fn unsaved_record_withheld() {
    let store = || {
        let mut finalize = finalize(FinalizeGoal::Walltime);
        finalize.step = Step::Store(Arc::new(record()));
        finalize
    };
    let retry = JobError::retryable("slow");
    let failed = child(Err::<[u8; 32], _>(retry.clone()));
    assert_eq!(store().next(failed), Ok(parked(retry)));
    let error = JobError::permanent("bad");
    let failed = child(Err::<[u8; 32], _>(error.clone()));
    let empty = execution_result("", None, Vec::new(), LogTails::default());
    assert_eq!(
        store().next(failed),
        Ok(ended(RunEnding::Fail {
            error,
            result: empty,
        }))
    );
}

// The exit code stays readable however long the backend detail is.
#[test]
fn exit_message_bounded() {
    assert_eq!(exit_message(2, Some("  ")), "container exited with code 2");
    let detail = "x".repeat(MAX_MESSAGE_BYTES * 2);
    let message = exit_message(2, Some(&detail));
    assert!(message.starts_with("container exited with code 2: "));
    assert!(message.len() <= MAX_MESSAGE_BYTES);
}
