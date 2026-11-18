//! Runs the import job through its checkpoint, stops, phases and failures.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::super::ImportInput;
use super::super::tests::{entry, plan, run, stops};
use super::*;
use crate::jobs::repository::test_support::{artifact, child};
use aruna_core::structs::execution::job::JobResultPayload;

fn job(phase: ImportPhase, step: Step) -> ImportJobOperation {
    let mut job = ImportJobOperation::new(run());
    job.checkpoint.phase = phase;
    job.confirmed = true;
    job.step = step;
    job
}

fn saved() -> IoResult {
    child(Ok::<_, JobMutationError>(()))
}

fn reported_event() -> IoResult {
    let event = aruna_core::events::Event::Job(aruna_core::jobs::JobEvent::Reported);
    IoResult::Event(Box::new(event))
}

fn stops_call() -> Turn {
    Ok(Next::Call(job_call(JobEffect::Stops)))
}

// A new job starts from an empty checkpoint, saves it and needs no plan.
#[test]
fn fresh_job_starts() {
    let mut job = ImportJobOperation::new(run());
    assert!(matches!(job.first(), Ok(Next::Call(IoCall::Child(_)))));
    let none = child(Ok::<Option<ImportCheckpoint>, StateReadError>(None));
    assert!(matches!(job.next(none), Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(job.step, Step::Saved { resumed: true });
    assert_eq!(job.next(saved()), stops_call());
}

// An unreadable checkpoint leaves the job to a later attempt.
#[test]
fn broken_checkpoint_retries() {
    let mut job = ImportJobOperation::new(run());
    let broken = child(Err::<Option<ImportCheckpoint>, _>(StateReadError(
        "broken".to_string(),
    )));
    assert_eq!(job.next(broken), give_up("broken"));
}

// A resumed write needs its plan, and a missing one fails the job for good.
#[test]
fn write_needs_plan() {
    let mut job = job(ImportPhase::Write, Step::Saved { resumed: true });
    assert!(matches!(
        job.next(saved()),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(job.step, Step::Plan { required: true });
    let none = child(Ok::<Option<ImportPlan>, StateReadError>(None));
    assert_eq!(
        job.next(none),
        Ok(Next::Done(JobRunOutcome::Failed(JobError::permanent(
            "import plan is missing"
        ))))
    );
}

// A cleanup resumed before planning goes on without a plan.
#[test]
fn resumed_cleanup_continues() {
    let mut job = job(ImportPhase::Cleanup, Step::Plan { required: false });
    let none = child(Ok::<Option<ImportPlan>, StateReadError>(None));
    assert_eq!(job.next(none), stops_call());
}

// A shutdown hands the job back, whatever its phase.
#[test]
fn shutdown_interrupts() {
    let mut job = job(ImportPhase::Write, Step::Stops);
    assert_eq!(
        job.next(stops(false, true)),
        Ok(Next::Done(JobRunOutcome::Interrupted))
    );
}

// A cancel marks the open entries and turns to cleanup.
#[test]
fn cancel_turns_cleanup() {
    let mut job = job(ImportPhase::Write, Step::Stops);
    job.plan = Some(plan(&["a"]));
    assert!(matches!(
        job.next(stops(true, false)),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert!(job.checkpoint.cancelled);
    assert_eq!(job.step, Step::Marked);
    assert!(matches!(
        job.next(child(Ok::<_, ImportFailure>(()))),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(job.checkpoint.phase, ImportPhase::Cleanup);
    assert_eq!(job.step, Step::Saved { resumed: false });
    assert_eq!(job.next(saved()), stops_call());
}

// A cleanup that is already running ignores the cancel.
#[test]
fn cleanup_ignores_cancel() {
    let mut job = job(ImportPhase::Cleanup, Step::Stops);
    assert!(matches!(
        job.next(stops(true, false)),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(job.step, Step::Phase);
    assert!(!job.checkpoint.cancelled);
}

// The first step of a run confirms the source; cleanup does not ask.
#[test]
fn source_confirmed_once() {
    let mut job = job(ImportPhase::Acquire, Step::Stops);
    job.confirmed = false;
    assert!(matches!(
        job.next(stops(false, false)),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(job.step, Step::Recheck);
    let rechecked = child(Ok::<_, ImportError>(()));
    assert!(matches!(
        job.next(rechecked),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(job.step, Step::Confirm);
    let confirmed = child(Ok::<_, ImportError>(()));
    assert!(matches!(
        job.next(confirmed),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    assert_eq!(job.step, Step::Report);
    assert!(job.confirmed);
}

// An unreachable source defers the job instead of spending an attempt.
#[test]
fn unreachable_source_defers() {
    let mut job = job(ImportPhase::Acquire, Step::Confirm);
    let error = ImportError::Unreachable("offline".to_string());
    let message = error.to_string();
    let answer = child(Err::<(), _>(error));
    assert_eq!(
        job.next(answer),
        Ok(Next::Done(JobRunOutcome::Deferred(JobError::retryable(
            message
        ))))
    );
}

// An acquired source is checkpointed and inspected next.
#[test]
fn acquired_source_advances() {
    let mut job = job(ImportPhase::Acquire, Step::Phase);
    let input = ImportInput {
        location: artifact().location,
        size: 4,
        blake3: [1; 32],
        upload_id: None,
        eln: false,
    };
    let acquired = AcquiredSource {
        input: input.clone(),
        identifiers: Vec::new(),
        pull: None,
    };
    let answer = child(Ok::<_, ImportFailure>(acquired));
    assert!(matches!(
        job.next(answer),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    assert_eq!(job.checkpoint.phase, ImportPhase::Inspect);
    assert_eq!(
        job.checkpoint.refs.hidden_locations,
        vec![input.location.clone()]
    );
    assert_eq!(job.checkpoint.input, Some(input));
    assert!(matches!(
        job.next(reported_event()),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(job.step, Step::Saved { resumed: false });
}

// A phase without its plan fails like any permanent failure.
#[test]
fn missing_plan_fails() {
    let mut job = job(ImportPhase::Rewrite, Step::Stops);
    assert!(matches!(
        job.next(stops(false, false)),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(job.step, Step::Rows(None));
    assert_eq!(
        job.checkpoint.failure,
        Some("import plan is missing".to_string())
    );
}

// A retryable failure ends the attempt and keeps the checkpoint.
#[test]
fn retryable_failure_ends() {
    let mut job = job(ImportPhase::Write, Step::Phase);
    let failed = child(Err::<ImportCheckpoint, _>(ImportFailure::Retryable(
        "busy".to_string(),
    )));
    assert_eq!(job.next(failed), give_up("busy"));
    assert_eq!(job.checkpoint.failure, None);
}

// A failed write reports its phase, marks the open entries and turns to cleanup.
#[test]
fn write_failure_marks() {
    let mut job = job(ImportPhase::Write, Step::Phase);
    job.plan = Some(plan(&["a"]));
    let failed = child(Err::<ImportCheckpoint, _>(ImportFailure::Permanent(
        "broken".to_string(),
    )));
    assert!(matches!(job.next(failed), Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(job.step, Step::Rows(Some("broken".to_string())));
    assert_eq!(job.checkpoint.failure, Some("broken".to_string()));
    let rows = child(Ok::<_, ImportFailure>(()));
    assert!(matches!(job.next(rows), Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(job.step, Step::Marked);
}

// A failure outside the write goes to cleanup without marking entries.
#[test]
fn create_failure_cleans() {
    let mut job = job(ImportPhase::Create, Step::Rows(None));
    let rows = child(Ok::<_, ImportFailure>(()));
    assert!(matches!(job.next(rows), Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(job.checkpoint.phase, ImportPhase::Cleanup);
}

// A finished entry moves the job's progress before it is saved.
#[test]
fn progress_reported() {
    let mut job = job(ImportPhase::Write, Step::Phase);
    job.plan = Some(plan(&["a"]));
    let done = ImportCheckpoint {
        phase: ImportPhase::Write,
        next_entry: 1,
        entries_total: 1,
        imported: 1,
        ..Default::default()
    };
    let answer = child(Ok::<_, ImportFailure>(done));
    let report = job_call(JobEffect::Report(JobReport {
        current: Some(1),
        total: Some(1),
        advance: 0,
        checkpoint: false,
    }));
    assert_eq!(job.next(answer), Ok(Next::Call(report)));
    assert_eq!(job.plan.as_ref().unwrap().entries, vec![entry("a")]);
}

// A finished import ends with its counts.
#[test]
fn done_import_succeeds() {
    let mut job = job(ImportPhase::Done, Step::Stops);
    job.checkpoint.imported = 2;
    let Ok(Next::Done(JobRunOutcome::Succeeded(JobResultPayload::ImportRoCrate(result)))) =
        job.next(stops(false, false))
    else {
        panic!("expected the import result");
    };
    assert_eq!(result.imported, 2);
}
