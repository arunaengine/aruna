//! Call and result tests of cancelling jobs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::execution::job::{
    ComputeResources, EffectiveResources, ExecutionSpec, SubmissionId,
};
use ulid::Ulid;

use super::*;
use crate::jobs::job_reads::tests::{
    child, is_child, job_id, node, owner, probe, realm, record_of, stranger,
};

fn found(record: Option<JobRecord>) -> IoResult {
    child(Ok::<_, JobReadError>(record))
}

fn requested(outcome: CancelRequestOutcome) -> IoResult {
    child(Ok::<_, JobMutationError>(outcome))
}

fn flagging() -> CancelOwnedOperation {
    let mut cancel = CancelOwnedOperation::new(owner(), job_id(), 5);
    assert!(is_child(
        &cancel.next(found(Some(record_of(probe())))).unwrap()
    ));
    cancel
}

fn family() -> JobFamilyId {
    JobFamilyId {
        submission_id: SubmissionId([7u8; 32]),
        request_digest: [8u8; 32],
    }
}

fn reservation(logical: u8, physical: u8) -> JobReservationRecord {
    JobReservationRecord {
        execution_id: Ulid::from_bytes([physical; 16]),
        job_id: JobId::from_bytes([physical; 16]),
        logical_job_id: JobId::from_bytes([logical; 16]),
        resources: EffectiveResources {
            cpu_cores: 1,
            ram_bytes: 0,
            disk_bytes: 0,
            max_walltime_ms: 0,
            preemptible: false,
        },
        created_at_ms: 1,
        subject_generation: 0,
        subject_digest: [0u8; 32],
    }
}

fn held(rows: Vec<JobReservationRecord>) -> IoResult {
    child(Ok::<_, LifecycleError>(rows))
}

fn alias(family: Option<JobFamilyId>) -> IoResult {
    child(Ok::<_, JobRouteError>(family))
}

fn routed() -> RoutedCancelOperation {
    let serving = Some((node(1), realm()));
    RoutedCancelOperation::new(owner(), job_id(), None, serving, 5)
}

fn status() -> RoutedJobStatus {
    RoutedJobStatus {
        job: JobStatusView::from(&record_of(probe())),
        run_crate: None,
    }
}

// A job the caller does not own is absent and is never flagged.
#[test]
fn unowned_not_flagged() {
    let mut cancel = CancelOwnedOperation::new(owner(), job_id(), 5);
    assert!(is_child(&cancel.first().unwrap()));
    let done = cancel.next(found(None)).unwrap();
    assert!(matches!(
        done,
        Next::Done(CancelReport {
            outcome: CancelJobOutcome::NotFound,
            ..
        })
    ));
}

// A flagged job names the attempt to signal and wakes the drain after the commit.
#[test]
fn flagged_signals_attempt() {
    let flagged = CancelRequestOutcome::Flagged(record_of(probe()));
    let next = flagging().next(requested(flagged)).unwrap();
    let Next::Then { output, calls } = next else {
        panic!("expected the drain wake");
    };
    assert_eq!(output.running, vec![job_id()]);
    assert!(matches!(output.outcome, CancelJobOutcome::Requested(_)));
    assert_eq!(calls.len(), 1);
}

// A job settled by the request needs no signal, and a pruned one is absent.
#[test]
fn settled_needs_nothing() {
    let cancelled = CancelRequestOutcome::Cancelled(record_of(probe()));
    let done = flagging().next(requested(cancelled)).unwrap();
    assert!(matches!(
        done,
        Next::Done(CancelReport { outcome: CancelJobOutcome::Requested(_), running })
            if running.is_empty()
    ));
    let terminal = CancelRequestOutcome::AlreadyTerminal(record_of(probe()));
    assert!(matches!(
        flagging().next(requested(terminal)).unwrap(),
        Next::Done(CancelReport {
            outcome: CancelJobOutcome::AlreadyTerminal(_),
            ..
        })
    ));
    let pruned = child(Err::<CancelRequestOutcome, _>(JobMutationError::NotFound));
    assert!(matches!(
        flagging().next(pruned).unwrap(),
        Next::Done(CancelReport {
            outcome: CancelJobOutcome::NotFound,
            ..
        })
    ));
}

// A settled execution wakes the drain for its run crate.
#[test]
fn settled_execution_wakes() {
    let spec = ExecutionSpec {
        group_id: Ulid::from_bytes([3u8; 16]),
        name: None,
        description: None,
        tags: Default::default(),
        image: "alpine:3".to_string(),
        entrypoint: None,
        command: Vec::new(),
        workdir: None,
        env: Default::default(),
        resources: ComputeResources::default(),
        executor_constraint: None,
        inputs: Vec::new(),
        file_outputs: Vec::new(),
        workspace_outputs: Vec::new(),
        output_prefixes: Vec::new(),
        collision_policy: Default::default(),
    };
    let cancelled = CancelRequestOutcome::Cancelled(record_of(JobPayload::Execution(spec)));
    let next = flagging().next(requested(cancelled)).unwrap();
    assert!(matches!(next, Next::Then { output, .. } if output.running.is_empty()));
}

// Without a family only the reservations of the alias itself match.
#[test]
fn runs_match_alias() {
    let alias_id = JobId::from_bytes([1u8; 16]);
    let mut runs = MatchingRunsOperation::new(alias_id, None);
    assert!(is_child(&runs.first().unwrap()));
    let rows = vec![reservation(1, 2), reservation(3, 4)];
    let done = runs.next(held(rows)).unwrap();
    assert!(matches!(done, Next::Done(jobs) if jobs == vec![JobId::from_bytes([2u8; 16])]));
}

// With a family every reservation is compared through its alias, in key order.
#[test]
fn runs_match_family() {
    let mut runs = MatchingRunsOperation::new(job_id(), Some(family()));
    let rows = vec![reservation(1, 2), reservation(3, 4), reservation(5, 6)];
    assert!(is_child(&runs.next(held(rows)).unwrap()));
    assert!(is_child(&runs.next(alias(Some(family()))).unwrap()));
    assert!(is_child(&runs.next(alias(None)).unwrap()));
    let done = runs.next(alias(Some(family()))).unwrap();
    let expected = vec![JobId::from_bytes([2u8; 16]), JobId::from_bytes([6u8; 16])];
    assert!(matches!(done, Next::Done(jobs) if jobs == expected));
}

// Without a network the job is cancelled here and its report passes through.
#[test]
fn routed_without_network() {
    let mut cancel = RoutedCancelOperation::new(owner(), job_id(), None, None, 5);
    assert!(is_child(&cancel.first().unwrap()));
    assert_eq!(cancel.step, RoutedStep::Local);
    assert_eq!(cancel.auth.realm_id, owner().realm_id);
    let report = CancelReport {
        outcome: CancelJobOutcome::Requested(record_of(probe())),
        running: vec![job_id()],
    };
    let done = cancel.next(child(Ok::<_, JobReadError>(report))).unwrap();
    assert!(matches!(
        done,
        Next::Done(CancelReport { outcome: RoutedCancelOutcome::Requested(_), running })
            if running == vec![job_id()]
    ));
}

// A family cancel stops the local runs of that family and wakes the drain.
#[test]
fn family_stops_runs() {
    let mut cancel = routed();
    assert!(is_child(&cancel.first().unwrap()));
    let published = child(Ok::<_, JobRouteError>(Some(())));
    assert!(is_child(&cancel.next(published).unwrap()));
    let projected = child(Ok::<_, JobRouteError>(Some(status())));
    assert!(is_child(&cancel.next(projected).unwrap()));
    assert!(is_child(&cancel.next(alias(Some(family()))).unwrap()));
    let runs = child(Ok::<_, JobRouteError>(vec![job_id()]));
    let Next::Then { output, calls } = cancel.next(runs).unwrap() else {
        panic!("expected the drain wake");
    };
    assert_eq!(output.running, vec![job_id()]);
    assert!(matches!(output.outcome, RoutedCancelOutcome::Requested(_)));
    assert_eq!(calls.len(), 1);
}

// A family that cannot be projected or resolved after the cancel is unavailable.
#[test]
fn family_cancel_incomplete() {
    let mut cancel = routed();
    cancel.step = RoutedStep::Status;
    let missing = child(Ok::<Option<RoutedJobStatus>, JobRouteError>(None));
    assert!(matches!(
        cancel.next(missing),
        Err(JobRouteError::Unavailable(_))
    ));
    let mut cancel = routed();
    let outcome = RoutedCancelOutcome::NotFound;
    cancel.step = RoutedStep::Alias(Box::new(outcome));
    assert!(matches!(
        cancel.next(alias(None)),
        Err(JobRouteError::Unavailable(_))
    ));
}

// Only the caller's visible job is accepted from a remote owner.
#[test]
fn remote_answer_checked() {
    let view = JobStatusView::from(&record_of(probe()));
    let answer = |terminal| {
        child(Ok::<_, JobRouteError>(JobRouteOutcome::Remote(
            JobResponse::Cancelled {
                job: view.clone(),
                terminal,
            },
        )))
    };
    let mut cancel = routed();
    let none = child(Ok::<Option<()>, JobRouteError>(None));
    assert!(is_child(&cancel.next(none).unwrap()));
    assert!(matches!(
        cancel.next(answer(true)),
        Ok(Next::Done(CancelReport {
            outcome: RoutedCancelOutcome::AlreadyTerminal(_),
            ..
        }))
    ));
    let mut foreign = RoutedCancelOperation::new(stranger(), job_id(), None, None, 5);
    foreign.step = RoutedStep::Route;
    assert!(matches!(
        foreign.next(answer(false)),
        Err(JobRouteError::Unavailable(_))
    ));
}
