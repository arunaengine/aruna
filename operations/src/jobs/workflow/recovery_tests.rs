//! Tests how an ambiguous submit recovers, by the calls and results of its operation.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::compute::{
    AdoptableEvidence, ArtifactEvidence, AttemptPhase, ResumePoint, StagingMode,
};
use aruna_core::structs::execution::job::AttemptIntent;

use super::super::fixtures::{
    EPOCH, child, compute, control, job, record, scope, status, stops, token,
};
use super::*;

const NOW: u64 = 7;
const PINNED: &str = "alpine@sha256:1";

fn lost() -> BackendError {
    BackendError::Unavailable("lost".to_string())
}

fn refused() -> BackendError {
    BackendError::InvalidSpec("bad".to_string())
}

fn pinned_record() -> JobRecord {
    let mut job = record();
    job.attempt_intent = Some(AttemptIntent {
        attempt_no: 1,
        external_name: "attempt".to_string(),
        executor_kind: "docker".to_string(),
        pinned_image: PINNED.to_string(),
        attempt_epoch: EPOCH,
    });
    job
}

fn recover(error: BackendError) -> RecoverSubmitOperation {
    let mut recover = RecoverSubmitOperation::new(scope(), error);
    recover.stamp(NOW);
    recover
}

/// A recovery that has read this job and asked the backend what it holds.
fn reconciling(error: BackendError, job: JobRecord) -> RecoverSubmitOperation {
    let mut recover = recover(error);
    recover.first().unwrap();
    recover.next(stops(false)).unwrap();
    let read = child(Ok::<_, JobMutationError>(Some(job)));
    assert_eq!(
        recover.next(read),
        Ok(Next::Call(scope().backend(ComputeRequest::Reconcile)))
    );
    recover
}

fn evidence(evidence: ReconcileEvidence) -> IoResult {
    compute(ComputeAnswer::Reconciled(evidence))
}

fn adoptable(phase: AttemptPhase) -> IoResult {
    evidence(ReconcileEvidence::Adoptable(AdoptableEvidence {
        status: status(phase),
        resume: ResumePoint::Submit,
    }))
}

fn tombstone() -> TombstoneEvidence {
    TombstoneEvidence {
        backend_ref: "stone".to_string(),
        attempt_epoch: EPOCH,
    }
}

fn prepared() -> PreparedTask {
    PreparedTask {
        inputs: Vec::new(),
        mounts: Vec::new(),
        secrets: BTreeMap::new(),
        staging: StagingMode::Files,
        workspace: None,
    }
}

fn finalize(goal: FinalizeGoal) -> Next<bool> {
    Next::Call(IoCall::child(FinalizeOperation::new(scope(), goal)))
}

fn parked(error: &BackendError) -> Next<bool> {
    let message = format!("submit failed ambiguously: {error}");
    let park = ParkAttemptOperation::new(
        job(),
        token(),
        JobError::retryable(message),
        scope().settle(NOW),
    );
    Next::Call(IoCall::child(park))
}

fn running_write() -> Next<bool> {
    let write = begin_external_running(job(), token(), None, NOW);
    Next::Call(IoCall::child(write))
}

fn wrote_running() -> IoResult {
    child(Ok::<_, JobMutationError>(record()))
}

fn tombstone_write() -> Next<bool> {
    let write = record_attempt_tombstone(job(), token(), EPOCH, "stone".to_string());
    Next::Call(IoCall::child(write))
}

fn prepare_call() -> Next<bool> {
    let scope = scope();
    let job = Arc::new(pinned_record());
    let run = scope.run(&job).unwrap();
    let prepare =
        PrepareAttemptOperation::new(run, (scope.spec.clone(), job), &scope.bucket, None, false);
    Next::Call(IoCall::child(prepare))
}

// A cancelled submit needs no evidence: the run is finalized as cancelled.
#[test]
fn cancelled_submit_finalizes() {
    let mut recover = recover(BackendError::Cancelled);
    assert_eq!(recover.first(), Ok(finalize(FinalizeGoal::Cancel)));
    let settled = child(Ok::<(), IoError>(()));
    assert_eq!(recover.next(settled), Ok(Next::Done(false)));
}

// A job told to stop, or one whose record asks for it, is cancelled before any backend call.
#[test]
fn cancel_precedes_reconcile() {
    let mut stopped = recover(lost());
    assert_eq!(stopped.first(), Ok(Next::Call(stops_call())));
    assert_eq!(
        stopped.next(stops(true)),
        Ok(finalize(FinalizeGoal::Cancel))
    );
    let mut requested = recover(lost());
    requested.first().unwrap();
    requested.next(stops(false)).unwrap();
    let mut job = record();
    job.cancel_requested = true;
    let read = child(Ok::<_, JobMutationError>(Some(job)));
    assert_eq!(requested.next(read), Ok(finalize(FinalizeGoal::Cancel)));
}

// Without a readable job there is nothing left to settle.
#[test]
fn missing_job_ends() {
    let missing = child(Ok::<Option<JobRecord>, JobMutationError>(None));
    let unreadable = child(Err::<Option<JobRecord>, _>(JobMutationError::NotFound));
    for read in [missing, unreadable] {
        let mut recover = recover(lost());
        recover.first().unwrap();
        recover.next(stops(false)).unwrap();
        assert_eq!(recover.next(read), Ok(Next::Done(false)));
    }
}

// A submit that did reach the backend is supervised like any running attempt.
#[test]
fn running_attempt_supervised() {
    let mut recover = reconciling(lost(), record());
    let found = adoptable(AttemptPhase::Running);
    assert_eq!(recover.next(found), Ok(running_write()));
    assert_eq!(recover.next(wrote_running()), Ok(Next::Done(true)));
}

// An attempt that already ended is finalized from the evidence found.
#[test]
fn ended_attempt_finalized() {
    let phase = AttemptPhase::Exited { code: 0 };
    let mut recover = reconciling(lost(), record());
    assert_eq!(recover.next(adoptable(phase.clone())), Ok(running_write()));
    assert_eq!(
        recover.next(wrote_running()),
        Ok(finalize(FinalizeGoal::Attempt(Ok(status(phase)))))
    );
}

// A cancel that lands while the attempt is marked running wins over supervising it.
#[test]
fn write_sees_cancel() {
    let mut recover = reconciling(lost(), record());
    recover.next(adoptable(AttemptPhase::Running)).unwrap();
    let mut job = record();
    job.cancel_requested = true;
    let wrote = child(Ok::<_, JobMutationError>(job));
    assert_eq!(recover.next(wrote), Ok(finalize(FinalizeGoal::Cancel)));
}

// A lost running write leaves the attempt to the next owner.
#[test]
fn lost_write_ends() {
    let mut recover = reconciling(lost(), record());
    recover.next(adoptable(AttemptPhase::Running)).unwrap();
    let lost_write = child(Err::<JobRecord, _>(JobMutationError::TokenMismatch));
    assert_eq!(recover.next(lost_write), Ok(Next::Done(false)));
}

// An absent attempt is submitted again under the same name, with the pinned image.
#[test]
fn absent_attempt_resubmits() {
    let scope = scope();
    let mut recover = reconciling(lost(), pinned_record());
    let absent = evidence(ReconcileEvidence::Absent);
    assert_eq!(recover.next(absent), Ok(prepare_call()));
    let task = build_task_spec(
        &scope.spec,
        &scope.fence.attempt,
        PINNED,
        prepared(),
        scope.run_as,
    );
    assert_eq!(
        recover.next(child(Ok::<_, JobError>(prepared()))),
        Ok(Next::Call(
            scope.backend(ComputeRequest::Submit(Box::new(task)))
        ))
    );
    let submitted = ComputeAnswer::Submitted(Ok(status(AttemptPhase::Submitted)));
    assert_eq!(recover.next(compute(submitted)), Ok(running_write()));
    assert_eq!(recover.next(wrote_running()), Ok(Next::Done(true)));
}

// An intent of another attempt must not launch anything; the conflict parks the run.
#[test]
fn foreign_intent_parks() {
    let mut recover = reconciling(lost(), record());
    recover.next(evidence(ReconcileEvidence::Absent)).unwrap();
    let conflict = BackendError::Conflict("attempt intent mismatch".to_string());
    assert_eq!(
        recover.next(child(Ok::<_, JobError>(prepared()))),
        Ok(parked(&conflict))
    );
}

// A preparation that fails again keeps the retry open by parking.
#[test]
fn failed_prepare_parks() {
    let mut recover = reconciling(lost(), pinned_record());
    recover.next(evidence(ReconcileEvidence::Absent)).unwrap();
    let failed = child(Err::<PreparedTask, _>(JobError::retryable("no input")));
    let error = BackendError::Unavailable("no input".to_string());
    assert_eq!(recover.next(failed), Ok(parked(&error)));
}

/// A recovery whose repeated submit the backend just refused for good.
fn refused_again() -> RecoverSubmitOperation {
    let mut recover = reconciling(lost(), pinned_record());
    recover.next(evidence(ReconcileEvidence::Absent)).unwrap();
    recover.next(child(Ok::<_, JobError>(prepared()))).unwrap();
    let answer = ComputeAnswer::Submitted(Err(refused()));
    let spec = TombstoneSpec { terminal_ref: None };
    assert_eq!(
        recover.next(compute(answer)),
        Ok(Next::Call(scope().backend(ComputeRequest::Tombstone(spec))))
    );
    recover
}

// A permanent refusal retires the name with a tombstone and fails the job with that error.
#[test]
fn refused_submit_retires() {
    let mut recover = refused_again();
    let stone = ComputeAnswer::Tombstone(Ok(tombstone()));
    assert_eq!(recover.next(compute(stone)), Ok(tombstone_write()));
    let failure = SubmitFailureOperation::new(
        &pinned_record(),
        token(),
        submit_error(&refused()),
        false,
        scope().settle(NOW),
    );
    let recorded = child(Ok::<_, JobMutationError>(control()));
    assert_eq!(
        recover.next(recorded),
        Ok(Next::Call(IoCall::child(failure)))
    );
    let settled = child(Ok::<(), IoError>(()));
    assert_eq!(recover.next(settled), Ok(Next::Done(false)));
}

// Without a tombstone the name stays taken, so the attempt parks with the refusal.
#[test]
fn failed_tombstone_parks() {
    let mut recover = refused_again();
    let failed = ComputeAnswer::Tombstone(Err(lost()));
    assert_eq!(recover.next(compute(failed)), Ok(parked(&refused())));
}

// A tombstone found by reconcile is recorded, and a lost write of it ends the recovery.
#[test]
fn tombstoned_attempt_retired() {
    let mut recover = reconciling(lost(), record());
    let found = evidence(ReconcileEvidence::Tombstoned(tombstone()));
    assert_eq!(recover.next(found), Ok(tombstone_write()));
    let failed = child(Err::<AttemptControl, _>(JobMutationError::TokenMismatch));
    assert_eq!(recover.next(failed), Ok(Next::Done(false)));
}

fn artifact(exact_identity: bool) -> ReconcileEvidence {
    ReconcileEvidence::Unadoptable(ArtifactEvidence {
        artifact_kind: "helper".to_string(),
        backend_ref: None,
        observed_epoch: Some(EPOCH),
        observed_generation: Some(1),
        exact_identity,
        multiple: false,
        foreign: false,
    })
}

// An artifact that is surely ours is stopped first, and the attempt parks either way.
#[test]
fn exact_artifact_stopped() {
    let mut recover = reconciling(lost(), record());
    assert_eq!(
        recover.next(evidence(artifact(true))),
        Ok(Next::Call(scope().backend(ComputeRequest::Cancel)))
    );
    let stopped = ComputeAnswer::Cancelled(Err(lost()));
    assert_eq!(recover.next(compute(stopped)), Ok(parked(&lost())));
}

// Evidence that proves nothing parks the attempt, as does a backend that is gone.
#[test]
fn unclear_evidence_parks() {
    let answers = [
        evidence(artifact(false)),
        evidence(ReconcileEvidence::Unavailable(lost())),
        compute(ComputeAnswer::NoBackend),
    ];
    for answer in answers {
        let mut recover = reconciling(lost(), record());
        assert_eq!(recover.next(answer), Ok(parked(&lost())));
    }
}

// Only permanent errors say so in the job's error kind.
#[test]
fn submit_error_classified() {
    assert_eq!(submit_error(&lost()).kind, JobError::retryable("").kind);
    assert_eq!(submit_error(&refused()).kind, JobError::permanent("").kind);
}
