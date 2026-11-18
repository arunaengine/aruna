//! Call and result tests of deleting a finished run.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::jobs::job_reads::tests::{
    auth_of, child, is_child, job_id, owner, probe, record_of, stored,
};

fn delete() -> DeleteOwnedOperation {
    DeleteOwnedOperation::new(&auth_of(owner()), job_id(), None, 5)
}

fn removed(outcome: RunDelete) -> IoResult {
    child(Ok::<_, JobMutationError>(outcome))
}

// A job that is no finished run is removed without a family record.
#[test]
fn plain_job_unrecorded() {
    let mut run = delete();
    assert!(is_child(&run.first().unwrap()));
    assert!(is_child(
        &run.next(stored(Some(record_of(probe())))).unwrap()
    ));
    assert!(is_child(&run.next(stored(None)).unwrap()));
    assert_eq!(run.step, DeleteStep::Remove);
    let done = run.next(removed(RunDelete::NotFound)).unwrap();
    assert!(matches!(done, Next::Done(RunDelete::NotFound)));
}

// A finished run is recorded first, and a failed record keeps it listed.
#[test]
fn failed_record_keeps() {
    let mut run = delete();
    run.step = DeleteStep::Cleanup { deletable: true };
    assert!(is_child(&run.next(stored(None)).unwrap()));
    assert_eq!(run.step, DeleteStep::Publish);
    let failed = child(Err::<(), _>(JobRouteError::Unavailable("down".into())));
    let done = run.next(failed).unwrap();
    assert!(matches!(done, Next::Done(RunDelete::Unrecorded(_))));
}

// A cleanup still running keeps the family record back.
#[test]
fn live_cleanup_unrecorded() {
    let mut run = delete();
    run.step = DeleteStep::Cleanup { deletable: true };
    let cleanup = record_of(probe());
    assert!(is_child(&run.next(stored(Some(cleanup))).unwrap()));
    assert_eq!(run.step, DeleteStep::Remove);
}

// A queued job that settles on cancel is deleted on the second pass, and only then.
#[test]
fn queued_job_cancelled() {
    let mut run = delete();
    run.step = DeleteStep::Remove;
    let queued = RunDelete::Unfinished(JobState::Queued);
    assert!(is_child(&run.next(removed(queued)).unwrap()));
    assert!(matches!(run.step, DeleteStep::Flag(_)));
    let cancelled = CancelRequestOutcome::Cancelled(record_of(probe()));
    let settled = child(Ok::<_, JobMutationError>(cancelled));
    assert!(is_child(&run.next(settled).unwrap()));
    assert_eq!(run.step, DeleteStep::Record);
    assert!(run.cancelled);

    run.step = DeleteStep::Remove;
    let again = RunDelete::Unfinished(JobState::Queued);
    assert!(matches!(
        run.next(removed(again)),
        Ok(Next::Done(RunDelete::Unfinished(JobState::Queued)))
    ));
}

// A queued job that only got flagged keeps its first answer.
#[test]
fn flagged_job_stays() {
    let mut run = delete();
    run.step = DeleteStep::Flag(RunDelete::Unfinished(JobState::AwaitingKey));
    let flagged = CancelRequestOutcome::Flagged(record_of(probe()));
    let done = run.next(child(Ok::<_, JobMutationError>(flagged))).unwrap();
    assert!(matches!(
        done,
        Next::Done(RunDelete::Unfinished(JobState::AwaitingKey))
    ));
}

// A deleted run moves the prune timer, whose failure does not undo the delete.
#[test]
fn deleted_moves_timer() {
    let mut run = delete();
    run.step = DeleteStep::Remove;
    assert!(is_child(&run.next(removed(RunDelete::Deleted)).unwrap()));
    assert_eq!(run.step, DeleteStep::Timer(RunDelete::Deleted));
    let failed = child(Err::<(), _>(IoError::Unfinished));
    let done = run.next(failed).unwrap();
    assert!(matches!(done, Next::Done(RunDelete::Deleted)));
}

// Only the owner's finished execution counts as a deletable run.
#[test]
fn deletable_needs_execution() {
    let mut run = delete();
    let mut finished = record_of(probe());
    finished.state = JobState::Succeeded;
    run.next(stored(Some(finished))).unwrap();
    assert_eq!(run.step, DeleteStep::Cleanup { deletable: false });
}
