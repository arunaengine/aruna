//! Tests adoption of a lost attempt by the calls and results of its operations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::compute::{
    AdoptableEvidence, ArtifactEvidence, ResumePoint, StagingMode, TombstoneEvidence,
};

use super::super::fixtures::{
    EPOCH, PINNED, child, claimed, compute, control, facts, intent, job, node, record, scope, spec,
    status, token,
};
use super::*;

const NOW: u64 = 5;

fn lost() -> BackendError {
    BackendError::Unavailable("lost".to_string())
}

fn running(started_at_ms: Option<u64>) -> AttemptStatus {
    let mut running = status(AttemptPhase::Running);
    running.started_at_ms = started_at_ms;
    running
}

fn adoptable(status: AttemptStatus) -> ReconcileEvidence {
    ReconcileEvidence::Adoptable(AdoptableEvidence {
        status,
        resume: ResumePoint::Observe,
    })
}

fn stored() -> IoResult {
    child(Ok::<_, JobMutationError>(record()))
}

fn settled() -> IoResult {
    child(Ok::<(), IoError>(()))
}

fn parked<T>(message: impl Into<String>) -> Next<T> {
    let error = JobError::retryable(message);
    let park = ParkAttemptOperation::new(job(), token(), error, scope().settle(NOW));
    Next::Call(IoCall::child(park))
}

fn finalized<T>(goal: FinalizeGoal) -> Next<T> {
    Next::Call(IoCall::child(FinalizeOperation::new(scope(), goal)))
}

fn adopt() -> AdoptAttemptOperation {
    let mut adopt = AdoptAttemptOperation::new(job(), node(), scope().wiring.clone());
    adopt.stamp(NOW);
    let write = adopt_external_attempt(job(), node(), NOW);
    assert_eq!(adopt.first(), Ok(Next::Call(IoCall::child(write))));
    adopt
}

fn adopted(job: JobRecord) -> IoResult {
    child(Ok::<_, JobMutationError>(AdoptOutcome::Adopted(
        job,
        control(),
    )))
}

fn with_intent() -> JobRecord {
    let mut job = claimed();
    job.attempt_intent = Some(intent());
    job
}

// A held or terminal attempt, a failed adoption and a job without a claim leave nothing to do.
#[test]
fn unadopted_attempt_ends() {
    let replies = [
        child(Ok::<_, JobMutationError>(AdoptOutcome::Skipped)),
        child(Err::<AdoptOutcome, _>(JobMutationError::NotFound)),
        adopted(record()),
    ];
    for reply in replies {
        assert_eq!(adopt().next(reply), Ok(Next::Done(Adoption::Done)));
    }
}

// An attempt that was never submitted goes back without charging an attempt.
#[test]
fn intentless_attempt_released() {
    let mut adopt = adopt();
    let release = release_job(job(), token(), NOW);
    assert_eq!(
        adopt.next(adopted(claimed())),
        Ok(Next::Call(IoCall::child(release)))
    );
    let released = child(Ok::<_, JobMutationError>(ReleaseOutcome::Skipped));
    assert_eq!(adopt.next(released), Ok(Next::Done(Adoption::Done)));
}

// The adopted scope is fenced by the stored intent and control, on the executor they name.
#[test]
fn adopted_attempt_scoped() {
    let mut adopt = adopt();
    let kind = ExecutorKind::Docker;
    assert_eq!(
        adopt.next(adopted(with_intent())),
        Ok(Next::Call(compute_call(ComputeEffect::Facts { kind })))
    );
    let mut expected = AttemptScope::clone(&scope());
    expected.fence.attempt = AttemptRef::new(job().to_string().to_lowercase(), 1);
    expected.bucket = String::new();
    assert_eq!(
        adopt.next(compute(ComputeAnswer::Facts(facts()))),
        Ok(Next::Done(Adoption::Adopted {
            scope: Arc::new(expected),
            record: Arc::new(with_intent()),
        }))
    );
}

// A node without the executor cannot observe the attempt, so it hands the lease back.
#[test]
fn missing_backend_handoff() {
    let mut adopt = adopt();
    adopt.next(adopted(with_intent())).unwrap();
    let handoff = handoff_external_attempt(job(), token(), NOW);
    assert_eq!(
        adopt.next(compute(ComputeAnswer::NoBackend)),
        Ok(Next::Call(IoCall::child(handoff)))
    );
    let failed = child(Err::<ReleaseOutcome, _>(JobMutationError::NotFound));
    assert_eq!(adopt.next(failed), Ok(Next::Done(Adoption::Done)));
}

fn settle_of(job: JobRecord) -> SettleAdoptedOperation {
    let mut settle = SettleAdoptedOperation::new(scope(), Arc::new(job));
    settle.stamp(NOW);
    let fence = scope().backend(ComputeRequest::Fence);
    assert_eq!(settle.first(), Ok(Next::Call(fence)));
    settle
}

/// A settlement of `job` whose fence held, waiting for the evidence.
fn reconciling(job: JobRecord) -> SettleAdoptedOperation {
    let mut settle = settle_of(job);
    let reconcile = scope().backend(ComputeRequest::Reconcile);
    assert_eq!(
        settle.next(compute(ComputeAnswer::Done(Ok(())))),
        Ok(Next::Call(reconcile))
    );
    settle
}

fn evidence(evidence: ReconcileEvidence) -> IoResult {
    compute(ComputeAnswer::Reconciled(evidence))
}

fn in_state(state: JobState) -> JobRecord {
    let mut job = record();
    job.state = state;
    job
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

// A fence that did not hold parks the attempt; nothing is read from the backend.
#[test]
fn failed_fence_parks() {
    let none = BackendError::Unavailable("no backend".to_string());
    let cases = [
        (ComputeAnswer::Done(Err(lost())), lost()),
        (ComputeAnswer::NoBackend, none),
    ];
    for (answer, error) in cases {
        let message = format!("reconcile fence failed: {error}");
        assert_eq!(
            settle_of(record()).next(compute(answer)),
            Ok(parked(message))
        );
    }
}

// A stored cancel is honored as soon as the fence holds.
#[test]
fn fenced_cancel_finalizes() {
    let mut job = record();
    job.cancel_requested = true;
    let held = compute(ComputeAnswer::Done(Ok(())));
    assert_eq!(
        settle_of(job).next(held),
        Ok(finalized(FinalizeGoal::Cancel))
    );
}

// A live attempt goes back to supervision, and an absent one is submitted under its name.
#[test]
fn unfinished_attempt_resumes() {
    let cases = [
        (adoptable(running(None)), AttemptPhase::Running),
        (ReconcileEvidence::Absent, AttemptPhase::Submitted),
    ];
    for (found, phase) in cases {
        let mut settle = reconciling(in_state(JobState::Running));
        assert_eq!(
            settle.next(evidence(found)),
            Ok(Next::Done(AdoptedEnd::Resume(phase)))
        );
    }
}

// Start evidence is stored first; losing that write parks the attempt.
#[test]
fn start_evidence_kept() {
    let started = || evidence(adoptable(running(Some(4))));
    let write = record_attempt_started(job(), token(), 4);
    let mut kept = reconciling(in_state(JobState::Running));
    assert_eq!(kept.next(started()), Ok(Next::Call(IoCall::child(write))));
    assert_eq!(
        kept.next(stored()),
        Ok(Next::Done(AdoptedEnd::Resume(AttemptPhase::Running)))
    );
    let mut lost_write = reconciling(in_state(JobState::Running));
    lost_write.next(started()).unwrap();
    let error = JobMutationError::TokenMismatch;
    let message = format!("start evidence write failed: {error}");
    let failed = child(Err::<JobRecord, _>(error));
    assert_eq!(lost_write.next(failed), Ok(parked(message)));
}

// An attempt found alive under a job not yet running is marked running first.
#[test]
fn found_attempt_marked() {
    let start = || {
        let mut settle = reconciling(in_state(JobState::Ready));
        let write = begin_external_running(job(), token(), None, NOW);
        assert_eq!(
            settle.next(evidence(adoptable(running(None)))),
            Ok(Next::Call(IoCall::child(write)))
        );
        settle
    };
    assert_eq!(
        start().next(stored()),
        Ok(Next::Done(AdoptedEnd::Resume(AttemptPhase::Running)))
    );
    let mut cancelling = record();
    cancelling.cancel_requested = true;
    let wrote = child(Ok::<_, JobMutationError>(cancelling));
    assert_eq!(start().next(wrote), Ok(finalized(FinalizeGoal::Cancel)));
    let error = JobMutationError::TokenMismatch;
    let message = format!("adopted attempt cannot run: {error}");
    let failed = child(Err::<JobRecord, _>(error));
    assert_eq!(start().next(failed), Ok(parked(message)));
}

// Terminal evidence is finalized, and a run past its walltime counts as failed.
#[test]
fn terminal_evidence_finalized() {
    let ended = |finished_at_ms| {
        let mut ended = status(AttemptPhase::Exited { code: 0 });
        ended.started_at_ms = Some(0);
        ended.finished_at_ms = Some(finished_at_ms);
        ended
    };
    let limit = DEFAULT_WALLTIME.as_millis() as u64;
    let mut failed = ended(limit + 1);
    failed.phase = AttemptPhase::Failed {
        reason: "walltime limit exceeded while unobserved".to_string(),
    };
    for (finished_at_ms, expected) in [(limit, ended(limit)), (limit + 1, failed)] {
        let attempt = over_walltime(ended(finished_at_ms), &spec());
        assert_eq!(attempt, expected);
    }
    let mut unstarted = status(AttemptPhase::Exited { code: 0 });
    unstarted.finished_at_ms = Some(limit * 2);
    let mut settle = reconciling(in_state(JobState::Running));
    assert_eq!(
        settle.next(evidence(adoptable(unstarted.clone()))),
        Ok(finalized(FinalizeGoal::Attempt(Ok(unstarted))))
    );
    assert_eq!(settle.next(settled()), Ok(Next::Done(AdoptedEnd::Settled)));
}

// Evidence that decides nothing parks, and an artifact that is ours is stopped first.
#[test]
fn unclear_evidence_parks() {
    let cases = [
        (
            evidence(ReconcileEvidence::Unavailable(lost())),
            format!("backend unavailable: {}", lost()),
        ),
        (
            evidence(artifact(false)),
            "backend artifact is not adoptable".to_string(),
        ),
        (
            compute(ComputeAnswer::NoBackend),
            "backend unavailable: no backend".to_string(),
        ),
    ];
    for (reply, message) in cases {
        let mut settle = reconciling(in_state(JobState::Running));
        assert_eq!(settle.next(reply), Ok(parked(message)));
    }
    let mut settle = reconciling(in_state(JobState::Running));
    assert_eq!(
        settle.next(evidence(artifact(true))),
        Ok(Next::Call(scope().backend(ComputeRequest::Cancel)))
    );
    let stopped = compute(ComputeAnswer::Cancelled(Err(lost())));
    assert_eq!(
        settle.next(stopped),
        Ok(parked("backend artifact is not adoptable"))
    );
}

// A tombstone is recorded and the attempt fails as a submit that may be retried.
#[test]
fn tombstone_fails_submit() {
    let job_record = in_state(JobState::Running);
    let mut settle = reconciling(job_record.clone());
    let stone = TombstoneEvidence {
        backend_ref: "stone".to_string(),
        attempt_epoch: EPOCH,
    };
    let write = record_attempt_tombstone(job(), token(), EPOCH, "stone".to_string());
    assert_eq!(
        settle.next(evidence(ReconcileEvidence::Tombstoned(stone))),
        Ok(Next::Call(IoCall::child(write)))
    );
    let error = JobError::retryable(format!(
        "submit failed: {}",
        BackendError::Unavailable("attempt tombstoned".to_string())
    ));
    let failure =
        SubmitFailureOperation::new(&job_record, token(), error, false, scope().settle(NOW));
    let recorded = child(Ok::<_, JobMutationError>(control()));
    assert_eq!(
        settle.next(recorded),
        Ok(Next::Call(IoCall::child(failure)))
    );
}

fn pinned() -> JobRecord {
    let mut job = record();
    job.attempt_intent = Some(intent());
    job
}

fn resume_of(job: JobRecord, phase: AttemptPhase) -> ResumeAttemptOperation {
    let mut resume = ResumeAttemptOperation::new(scope(), Arc::new(job), phase);
    resume.stamp(NOW);
    let read = IoCall::child(read_job_record(record().job_id));
    assert_eq!(resume.first(), Ok(Next::Call(read)));
    resume
}

fn current(job: JobRecord) -> IoResult {
    child(Ok::<_, JobMutationError>(Some(job)))
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

/// A resume of a never started attempt, waiting for its preparation.
fn preparing(job: JobRecord) -> ResumeAttemptOperation {
    let mut resume = resume_of(job.clone(), AttemptPhase::Submitted);
    let scope = scope();
    let held = Arc::new(job.clone());
    let prepare = PrepareAttemptOperation::new(
        scope.run(&held).unwrap(),
        (scope.spec.clone(), held),
        &scope.bucket,
        None,
        false,
    );
    assert_eq!(
        resume.next(current(job)),
        Ok(Next::Call(IoCall::child(prepare)))
    );
    resume
}

fn supervised() -> Next<()> {
    Next::Call(IoCall::child(SuperviseOperation::new(scope())))
}

fn running_write(started_at_ms: Option<u64>) -> Next<()> {
    let write = begin_external_running(job(), token(), started_at_ms, NOW);
    Next::Call(IoCall::child(write))
}

// The job as it stands now decides: a cancel wins, a gone job ends, a live attempt is watched.
#[test]
fn current_job_decides() {
    let mut cancelling = record();
    cancelling.cancel_requested = true;
    let mut resume = resume_of(record(), AttemptPhase::Running);
    assert_eq!(
        resume.next(current(cancelling)),
        Ok(finalized(FinalizeGoal::Cancel))
    );
    let mut resume = resume_of(record(), AttemptPhase::Running);
    let gone = child(Ok::<Option<JobRecord>, JobMutationError>(None));
    assert_eq!(resume.next(gone), Ok(Next::Done(())));
    let mut resume = resume_of(record(), AttemptPhase::Running);
    assert_eq!(resume.next(current(record())), Ok(supervised()));
    assert_eq!(resume.next(settled()), Ok(Next::Done(())));
}

// A resubmit keeps the pinned image and the name, then supervises the attempt.
#[test]
fn resubmit_supervised() {
    let scope = scope();
    let task = build_task_spec(
        &scope.spec,
        &scope.fence.attempt,
        PINNED,
        prepared(),
        scope.run_as,
    );
    let mut resume = preparing(pinned());
    assert_eq!(
        resume.next(child(Ok::<_, JobError>(prepared()))),
        Ok(Next::Call(
            scope.backend(ComputeRequest::Submit(Box::new(task)))
        ))
    );
    let started = running(Some(4));
    let write = record_attempt_started(job(), token(), 4);
    assert_eq!(
        resume.next(compute(ComputeAnswer::Submitted(Ok(started)))),
        Ok(Next::Call(IoCall::child(write)))
    );
    assert_eq!(resume.next(stored()), Ok(running_write(Some(4))));
    assert_eq!(resume.next(stored()), Ok(supervised()));
}

// An intent of another fence never launches; failed preparation parks or fails by its kind.
#[test]
fn failed_resubmit_settles() {
    let mut foreign = preparing(record());
    assert_eq!(
        foreign.next(child(Ok::<_, JobError>(prepared()))),
        Ok(parked("attempt intent mismatch"))
    );
    let retry = JobError::retryable("slow");
    let reply = child(Err::<PreparedTask, _>(retry.clone()));
    assert_eq!(preparing(pinned()).next(reply), Ok(parked(retry.message)));
    let error = JobError::permanent("denied");
    let reply = child(Err::<PreparedTask, _>(error.clone()));
    let result = execution_result("", None, Vec::new(), Default::default());
    let ending = RunEnding::Fail { error, result };
    let end = EndRunOperation::new(job(), token(), ending, scope().settle(NOW));
    assert_eq!(
        preparing(pinned()).next(reply),
        Ok(Next::Call(IoCall::child(end)))
    );
}

/// A resume that just asked the backend to submit again.
fn submitting() -> ResumeAttemptOperation {
    let mut resume = preparing(pinned());
    resume.next(child(Ok::<_, JobError>(prepared()))).unwrap();
    resume
}

// The backend's answer to the resubmit decides how the attempt settles.
#[test]
fn submit_answer_settles() {
    let cancelled = ComputeAnswer::Submitted(Err(BackendError::Cancelled));
    assert_eq!(
        submitting().next(compute(cancelled)),
        Ok(finalized(FinalizeGoal::Cancel))
    );
    let failed = ComputeAnswer::Submitted(Err(lost()));
    assert_eq!(
        submitting().next(compute(failed)),
        Ok(parked(format!("resubmit failed: {}", lost())))
    );
    assert_eq!(
        submitting().next(compute(ComputeAnswer::NoBackend)),
        Ok(parked("resubmit failed: no backend"))
    );
}

// A claim another node took ends the resume without settling anything.
#[test]
fn lost_claim_ends() {
    let mut resume = submitting();
    let accepted = ComputeAnswer::Submitted(Ok(running(None)));
    assert_eq!(resume.next(compute(accepted)), Ok(running_write(None)));
    let lost_write = child(Err::<JobRecord, _>(JobMutationError::TokenMismatch));
    assert_eq!(resume.next(lost_write), Ok(Next::Done(())));
}
