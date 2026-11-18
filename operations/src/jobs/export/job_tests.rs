//! Runs the export job through its checkpoint, stops, phases and failures.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::events::Event;
use aruna_core::jobs::JobEvent;
use aruna_core::structs::execution::job::JobError;

use super::super::OpenStatus;
use super::super::tests::{export_scope, plain_entity};
use super::*;
use crate::jobs::repository::test_support::{artifact, child};

fn run(phase: ExportPhase, step: Step) -> ExportRunOperation {
    let mut run = ExportRunOperation::new(export_scope());
    run.checkpoint.phase = phase;
    run.step = step;
    run
}

fn stops(cancelled: bool, shutdown: bool) -> IoResult {
    let stops = JobStops {
        cancelled,
        shutdown,
    };
    IoResult::Event(Box::new(Event::Job(JobEvent::Stops(stops))))
}

fn saved() -> IoResult {
    child(Ok::<_, JobMutationError>(()))
}

fn stops_call() -> Turn {
    Ok(Next::Call(job_call(JobEffect::Stops)))
}

fn hidden_artifact() -> ArtifactRef {
    let mut artifact = artifact();
    artifact.location.backend_path = "_jobs/01ARZ3NDEKTSV4RRFFQ69G5FAV/rocrate.zip".to_string();
    artifact
}

// A new job goes straight to its first turn.
#[test]
fn fresh_job_starts() {
    let mut run = ExportRunOperation::new(export_scope());
    assert!(matches!(run.first(), Ok(Next::Call(IoCall::Child(_)))));
    let none = child(Ok::<Option<ExportCheckpoint>, StateReadError>(None));
    assert_eq!(run.next(none), stops_call());
}

// A resumed job shows its progress first.
#[test]
fn resumed_job_reports() {
    let mut run = ExportRunOperation::new(export_scope());
    let stored = ExportCheckpoint {
        entities: vec![plain_entity("a")],
        phase: ExportPhase::Plan,
        ..Default::default()
    };
    let answer = child(Ok::<_, StateReadError>(Some(stored)));
    let report = job_call(JobEffect::Report(JobReport {
        current: Some(1),
        total: Some(1),
        advance: 0,
        checkpoint: false,
    }));
    assert_eq!(run.next(answer), Ok(Next::Call(report)));
}

// An unreadable checkpoint leaves the job to a later attempt.
#[test]
fn broken_checkpoint_retries() {
    let mut run = ExportRunOperation::new(export_scope());
    let broken = child(Err::<Option<ExportCheckpoint>, _>(StateReadError(
        "broken".to_string(),
    )));
    assert_eq!(run.next(broken), ended(retryable("broken")));
}

// A shutdown hands the job back.
#[test]
fn shutdown_interrupts() {
    let mut run = run(ExportPhase::Resolve, Step::Stops);
    assert_eq!(
        run.next(stops(false, true)),
        ended(JobRunOutcome::Interrupted)
    );
}

// A cancel deletes the artifact, saves the checkpoint and ends the job.
#[test]
fn cancel_discards() {
    let mut run = run(ExportPhase::Publish, Step::Stops);
    run.checkpoint.artifact = Some(hidden_artifact());
    assert!(matches!(
        run.next(stops(true, false)),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    let deleted = IoResult::Event(Box::new(Event::Blob(
        aruna_core::events::BlobEvent::HiddenDeleted,
    )));
    assert!(matches!(
        run.next(deleted),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(run.checkpoint.artifact, None);
    assert_eq!(run.next(saved()), ended(JobRunOutcome::Cancelled));
}

// The snapshot sets the progress total before it is saved.
#[test]
fn snapshot_reports_total() {
    let mut run = run(ExportPhase::Snapshot, Step::Snapshot);
    let checkpoint = ExportCheckpoint {
        entities: vec![plain_entity("a")],
        phase: ExportPhase::Resolve,
        ..Default::default()
    };
    let answer = child(Ok::<_, ExportFailure>(checkpoint));
    let report = job_call(JobEffect::Report(JobReport {
        current: None,
        total: Some(1),
        advance: 0,
        checkpoint: false,
    }));
    assert_eq!(run.next(answer), Ok(Next::Call(report)));
    let reported = IoResult::Event(Box::new(Event::Job(JobEvent::Reported)));
    assert!(matches!(
        run.next(reported),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(run.step, Step::Saved);
    assert_eq!(run.next(saved()), stops_call());
}

// Resolved entities move the export on to its plan.
#[test]
fn resolved_plans() {
    let mut run = run(ExportPhase::Resolve, Step::Resolve);
    let resolved = Resolved {
        entities: vec![plain_entity("a")],
        aliases: AliasState::default(),
    };
    let answer = child(Ok::<_, ExportFailure>(resolved));
    assert!(matches!(run.next(answer), Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(run.checkpoint.phase, ExportPhase::Plan);
}

// A candidate that failed in the archive is set aside and the job takes another turn.
#[test]
fn candidate_failure_retries() {
    let mut run = run(ExportPhase::Assemble, Step::Assemble);
    let failed = child(Err::<ArtifactRef, _>(ExportFailure::Candidate {
        entity_index: 1,
        candidate_index: 2,
        status: OpenStatus::Offline,
        message: "gone".to_string(),
    }));
    assert_eq!(run.next(failed), stops_call());
    assert_eq!(run.failures[&1][&2], OpenStatus::Offline);
}

// The artifact is recorded as the hidden blob of the job.
#[test]
fn artifact_recorded() {
    let mut run = run(ExportPhase::Assemble, Step::Assemble);
    let answer = child(Ok::<_, ExportFailure>(hidden_artifact()));
    assert!(matches!(run.next(answer), Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(run.checkpoint.phase, ExportPhase::Publish);
    assert_eq!(run.checkpoint.refs.hidden_locations.len(), 1);
}

// Assembling again without probed sources probes first.
#[test]
fn assemble_probes_first() {
    let mut run = run(ExportPhase::Assemble, Step::Stops);
    assert!(matches!(
        run.next(stops(false, false)),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(run.step, Step::Probe);
}

// A publish without an artifact fails for good.
#[test]
fn publish_needs_artifact() {
    let mut run = run(ExportPhase::Publish, Step::Stops);
    assert_eq!(
        run.next(stops(false, false)),
        ended(permanent("export artifact is missing"))
    );
}

// The written rows end the job with its counts.
#[test]
fn rows_end_job() {
    let mut run = run(ExportPhase::Publish, Step::Rows(hidden_artifact()));
    let answer = child(Ok::<_, ExportFailure>(()));
    let Ok(Next::Done(JobRunOutcome::Succeeded(JobResultPayload::ExportRoCrate(result)))) =
        run.next(answer)
    else {
        panic!("expected the export result");
    };
    assert_eq!(result.artifact, Some(hidden_artifact()));
}

// A cancel while the rows are written discards the artifact.
#[test]
fn rows_cancelled_discards() {
    let mut run = run(ExportPhase::Publish, Step::Rows(hidden_artifact()));
    run.checkpoint.artifact = Some(hidden_artifact());
    let answer = child(Err::<(), _>(ExportFailure::Cancelled));
    assert!(matches!(
        run.next(answer),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
}

// A repository failure ends the job and keeps its artifact for the retry.
#[test]
fn repository_keeps_artifact() {
    let mut run = run(ExportPhase::Publish, Step::Repository);
    run.checkpoint.artifact = Some(hidden_artifact());
    let failed = child(Err::<ExportCheckpoint, _>(ExportFailure::Retryable(
        "busy".to_string(),
    )));
    assert_eq!(run.next(failed), ended(retryable("busy")));
    assert!(run.checkpoint.artifact.is_some());
}

// A failed save ends the attempt after the artifact is gone.
#[test]
fn failed_save_retries() {
    let mut run = run(ExportPhase::Plan, Step::Saved);
    let failed = child(Err::<(), _>(JobMutationError::NotFound));
    assert_eq!(
        run.next(failed),
        ended(retryable(JobMutationError::NotFound.to_string()))
    );
}

// A validation failure records its rows before the job fails.
#[test]
fn validation_writes_rows() {
    let mut run = run(ExportPhase::Snapshot, Step::Snapshot);
    let failed = child(Err::<ExportCheckpoint, _>(ExportFailure::Validation(
        Vec::new(),
    )));
    assert!(matches!(run.next(failed), Ok(Next::Call(IoCall::Child(_)))));
    assert!(matches!(run.step, Step::Validation(_)));
    let written = child(Ok::<_, ExportFailure>(()));
    assert_eq!(
        run.next(written),
        ended(JobRunOutcome::Failed(JobError::permanent("")))
    );
}

// Without a link the run's outcome is the job's outcome.
#[test]
fn unlinked_job_ends() {
    let mut job = ExportJobOperation::new(export_scope());
    assert!(matches!(job.first(), Ok(Next::Call(IoCall::Child(_)))));
    let answer = child(Ok::<_, ExportJobError>(JobRunOutcome::Interrupted));
    assert_eq!(job.next(answer), ended(JobRunOutcome::Interrupted));
}
