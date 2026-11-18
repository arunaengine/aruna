//! Tests how a running attempt is supervised, by the calls and results of its operations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::compute::runtimes::{SESSION_TAG, SESSION_TAG_NOTEBOOK};
use aruna_core::compute::{AttemptPhase, ExecutorKind};
use aruna_core::structs::execution::job::ExecutionSpec;

use super::super::fixtures::{
    child, compute, is_child, job, record, scope_of, spec, status, stops, token,
};
use super::*;

const NOW: u64 = 1_000;
const WALLTIME: u64 = 600;

fn limited() -> ExecutionSpec {
    let mut spec = spec();
    spec.resources.max_walltime_ms = Some(WALLTIME);
    spec
}

fn notebook() -> ExecutionSpec {
    let mut spec = limited();
    spec.tags
        .insert(SESSION_TAG.to_string(), SESSION_TAG_NOTEBOOK.to_string());
    spec
}

fn exited() -> AttemptStatus {
    status(AttemptPhase::Exited { code: 0 })
}

fn supervise(spec: ExecutionSpec) -> SuperviseOperation {
    let mut supervise = SuperviseOperation::new(scope_of(spec));
    supervise.stamp(NOW);
    supervise
}

fn anchor(started_at_ms: u64) -> IoResult {
    child(Ok::<_, IoError>(started_at_ms))
}

fn awaited(spec: ExecutionSpec, left_ms: u64, session_job: Option<String>) -> Next<()> {
    Next::Call(scope_of(spec).backend(ComputeRequest::Await {
        walltime_left: Duration::from_millis(left_ms),
        session_job,
    }))
}

fn finalized(spec: ExecutionSpec, goal: FinalizeGoal) -> Next<()> {
    let finalize = FinalizeOperation::new(scope_of(spec), goal);
    Next::Call(IoCall::child(finalize))
}

fn ended(end: AttemptEnd) -> IoResult {
    compute(ComputeAnswer::Ended(end))
}

fn settled() -> IoResult {
    child(Ok::<(), IoError>(()))
}

fn session_done() -> IoResult {
    child(Ok::<_, IoError>(SessionAnswer::Done))
}

fn session(request: SessionRequest) -> Next<()> {
    Next::Call(session_call(job(), request))
}

/// A plain run that is waiting for its attempt.
fn waiting() -> SuperviseOperation {
    let mut supervise = supervise(limited());
    supervise.first().unwrap();
    supervise.next(anchor(700)).unwrap();
    supervise
}

/// A run with an open session that is waiting for its attempt.
fn in_session() -> SuperviseOperation {
    let mut supervise = supervise(notebook());
    supervise.first().unwrap();
    let facts = ComputeEffect::Facts {
        kind: ExecutorKind::Docker,
    };
    assert_eq!(
        supervise.next(anchor(700)),
        Ok(Next::Call(compute_call(facts)))
    );
    assert!(is_child(&supervise.next(compute(ComputeAnswer::NoBackend))));
    let plan = SessionPlan {
        public_job_id: job(),
        credential_expires_ms: 5_000,
        idle_after_ms: 2_000,
    };
    let planned = supervise.next(child(Ok::<_, IoError>(Some(plan))));
    assert!(matches!(planned, Ok(Next::Call(IoCall::Effect(_)))));
    assert_eq!(
        supervise.next(compute(ComputeAnswer::Done(Ok(())))),
        Ok(awaited(notebook(), 300, Some(job().to_string())))
    );
    supervise
}

// The window left is what remains of the walltime since the anchor, never below zero.
#[test]
fn walltime_left_counted() {
    for (started_at_ms, left_ms) in [(700, 300), (100, 0)] {
        let mut supervise = supervise(limited());
        let first = WalltimeAnchorOperation::new(job(), token());
        assert_eq!(supervise.first(), Ok(Next::Call(IoCall::child(first))));
        assert_eq!(
            supervise.next(anchor(started_at_ms)),
            Ok(awaited(limited(), left_ms, None))
        );
    }
}

// An anchor that cannot be found starts the window now, and an unset limit uses the default.
#[test]
fn walltime_falls_back() {
    let mut failed = supervise(limited());
    failed.first().unwrap();
    let lost = child(Err::<u64, _>(IoError::Unfinished));
    assert_eq!(failed.next(lost), Ok(awaited(limited(), WALLTIME, None)));
    let mut unlimited = supervise(spec());
    unlimited.first().unwrap();
    let left = DEFAULT_WALLTIME.as_millis() as u64;
    assert_eq!(unlimited.next(anchor(NOW)), Ok(awaited(spec(), left, None)));
}

// Each way an attempt can end is finalized for that reason, and finalizing ends supervision.
#[test]
fn attempt_ends_finalize() {
    let gone = || {
        let error = BackendError::Unavailable("the attempt's backend is gone".to_string());
        FinalizeGoal::Attempt(Err(error))
    };
    let cases = [
        (
            ended(AttemptEnd::Attempt(Ok(exited()))),
            FinalizeGoal::Attempt(Ok(exited())),
        ),
        (ended(AttemptEnd::Walltime), FinalizeGoal::Walltime),
        (
            ended(AttemptEnd::SessionStopped(EndReason::Idle)),
            FinalizeGoal::Session(EndReason::Idle),
        ),
        (compute(ComputeAnswer::NoBackend), gone()),
    ];
    for (answer, goal) in cases {
        let mut supervise = waiting();
        assert_eq!(supervise.next(answer), Ok(finalized(limited(), goal)));
        assert_eq!(supervise.next(settled()), Ok(Next::Done(())));
    }
}

// A failed finalization is logged; supervision still ends.
#[test]
fn failed_finalize_ends() {
    let mut supervise = waiting();
    supervise.next(ended(AttemptEnd::Walltime)).unwrap();
    let failed = child(Err::<(), _>(IoError::Unfinished));
    assert_eq!(supervise.next(failed), Ok(Next::Done(())));
}

// A session run reports its end before the attempt is finalized, and closes last.
#[test]
fn session_ends_ordered() {
    for (cancelled, reason) in [(true, EndReason::Cancelled), (false, EndReason::KernelExit)] {
        let mut supervise = in_session();
        assert_eq!(
            supervise.next(ended(AttemptEnd::Attempt(Ok(exited())))),
            Ok(Next::Call(stops_call()))
        );
        assert_eq!(
            supervise.next(stops(cancelled)),
            Ok(session(SessionRequest::End(reason)))
        );
        let report = SessionReportOperation::new(job(), token(), true, reason);
        assert_eq!(
            supervise.next(session_done()),
            Ok(Next::Call(IoCall::child(report)))
        );
        assert_eq!(
            supervise.next(settled()),
            Ok(finalized(notebook(), FinalizeGoal::Attempt(Ok(exited()))))
        );
        assert_eq!(
            supervise.next(settled()),
            Ok(session(SessionRequest::Close))
        );
        assert_eq!(supervise.next(session_done()), Ok(Next::Done(())));
    }
}

// A passed walltime ends the session under that reason before the attempt is stopped.
#[test]
fn walltime_ends_session() {
    let mut supervise = in_session();
    assert_eq!(
        supervise.next(ended(AttemptEnd::Walltime)),
        Ok(session(SessionRequest::End(EndReason::Walltime)))
    );
    supervise.next(session_done()).unwrap();
    assert_eq!(
        supervise.next(settled()),
        Ok(finalized(notebook(), FinalizeGoal::Walltime))
    );
}

// A session the backend refused leaves a plain run with nothing to close.
#[test]
fn refused_session_skipped() {
    let mut supervise = supervise(notebook());
    supervise.first().unwrap();
    supervise.next(anchor(700)).unwrap();
    supervise.next(compute(ComputeAnswer::NoBackend)).unwrap();
    let plan = SessionPlan {
        public_job_id: job(),
        credential_expires_ms: 5_000,
        idle_after_ms: 2_000,
    };
    supervise.next(child(Ok::<_, IoError>(Some(plan)))).unwrap();
    let refused = Err(BackendError::Unavailable("no".to_string()));
    assert_eq!(
        supervise.next(compute(ComputeAnswer::Done(refused))),
        Ok(awaited(notebook(), 300, None))
    );
    supervise.next(ended(AttemptEnd::Walltime)).unwrap();
    assert_eq!(supervise.next(settled()), Ok(Next::Done(())));
}

// A session run with no plan, or on a node without an identity, only waits.
#[test]
fn unplanned_session_waits() {
    let mut unplanned = supervise(notebook());
    unplanned.first().unwrap();
    unplanned.next(anchor(700)).unwrap();
    unplanned.next(compute(ComputeAnswer::NoBackend)).unwrap();
    let no_plan = child(Ok::<Option<SessionPlan>, IoError>(None));
    assert_eq!(unplanned.next(no_plan), Ok(awaited(notebook(), 300, None)));

    let mut unserved = AttemptScope::clone(&scope_of(notebook()));
    unserved.wiring.serving = None;
    let unserved = Arc::new(unserved);
    let mut supervise = SuperviseOperation::new(unserved.clone());
    supervise.stamp(NOW);
    supervise.first().unwrap();
    let request = ComputeRequest::Await {
        walltime_left: Duration::from_millis(300),
        session_job: None,
    };
    assert_eq!(
        supervise.next(anchor(700)),
        Ok(Next::Call(unserved.backend(request)))
    );
}

fn anchoring() -> WalltimeAnchorOperation {
    let mut anchor = WalltimeAnchorOperation::new(job(), token());
    anchor.stamp(NOW);
    let read = IoCall::child(read_job_record(job()));
    assert_eq!(anchor.first(), Ok(Next::Call(read)));
    anchor
}

fn started_record(started_at_ms: Option<u64>) -> JobRecord {
    let mut job = record();
    job.started_at_ms = started_at_ms;
    job
}

// A stored start stays the anchor, so a restart cannot grant a fresh window.
#[test]
fn stored_anchor_kept() {
    let read = child(Ok::<_, JobMutationError>(Some(started_record(Some(40)))));
    assert_eq!(anchoring().next(read), Ok(Next::Done(40)));
}

// The first pass stores its clock, and a start another writer stored wins over it.
#[test]
fn first_anchor_written() {
    let cases = [
        (
            child(Ok::<_, JobMutationError>(started_record(Some(40)))),
            40,
        ),
        (child(Ok::<_, JobMutationError>(started_record(None))), NOW),
        (
            child(Err::<JobRecord, _>(JobMutationError::TokenMismatch)),
            NOW,
        ),
    ];
    for (written, anchored) in cases {
        let mut anchor = anchoring();
        let read = child(Ok::<_, JobMutationError>(Some(started_record(None))));
        let write = record_attempt_started(job(), token(), NOW);
        assert_eq!(anchor.next(read), Ok(Next::Call(IoCall::child(write))));
        assert_eq!(anchor.next(written), Ok(Next::Done(anchored)));
    }
}

// A session another node took over is ended as cancelled and closed here.
#[test]
fn abandoned_session_closed() {
    let mut abandon = AbandonSessionOperation::new(job());
    assert_eq!(
        abandon.first(),
        Ok(session(SessionRequest::End(EndReason::Cancelled)))
    );
    assert_eq!(
        abandon.next(session_done()),
        Ok(session(SessionRequest::Close))
    );
    assert_eq!(abandon.next(session_done()), Ok(Next::Done(())));
}
