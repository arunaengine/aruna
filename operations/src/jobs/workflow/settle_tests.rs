//! Tests how a run ends in the store, by the calls and results of its operations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::ChildOutcome;
use aruna_core::structs::identity::realm::RealmId;

use super::*;

const SCOPE: SettleScope = SettleScope {
    serving: None,
    now_ms: 9,
};

fn job() -> JobId {
    JobId::from_bytes([1; 16])
}

fn token() -> Ulid {
    Ulid::from_bytes([2; 16])
}

fn record() -> JobRecord {
    let node_id = iroh::SecretKey::from_bytes(&[4; 32]).public();
    let user = UserId::local(Ulid::from_bytes([5; 16]), RealmId([1; 32]));
    let payload = JobPayload::WriteRunCrate { for_job: job() };
    JobRecord::new(job(), payload, user, node_id, 1, 1, None)
}

fn result() -> JobResultPayload {
    execution_result("work", Some(1), Vec::new(), LogTails::default())
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn stored() -> IoResult {
    child(Ok::<_, JobMutationError>(record()))
}

fn announced() -> IoResult {
    child(Ok::<(), IoError>(()))
}

fn is_child(next: &Result<Next<()>, IoError>) -> bool {
    matches!(next, Ok(Next::Call(IoCall::Child(_))))
}

fn drain() -> IoCall {
    task_call(schedule_drain_effect())
}

fn settle_retry() -> IoCall {
    task_call(schedule_terminal_settle(SETTLE_RETRY_AFTER))
}

// A published terminal state only wakes the drain.
#[test]
fn announce_wakes_drain() {
    let mut announce = AnnounceTerminalOperation::new(record(), SCOPE);
    assert!(is_child(&announce.first()));
    let published = child(Ok::<_, LifecycleError>(true));
    assert_eq!(
        announce.next(published),
        Ok(Next::Then {
            output: (),
            calls: vec![drain()],
        })
    );
}

// An unpublished state also arms the settle task, first, however the publication failed.
#[test]
fn unpublished_arms_settle() {
    let failure = LifecycleError::from(IoError::Unfinished);
    for reply in [
        child(Ok::<_, LifecycleError>(false)),
        child(Err::<bool, _>(failure)),
    ] {
        let mut announce = AnnounceTerminalOperation::new(record(), SCOPE);
        announce.first().unwrap();
        assert_eq!(
            announce.next(reply),
            Ok(Next::Then {
                output: (),
                calls: vec![settle_retry(), drain()],
            })
        );
    }
}

// A terminal write this run won is announced.
#[test]
fn ending_announces() {
    let endings = [
        RunEnding::Fail {
            error: JobError::permanent("no"),
            result: result(),
        },
        RunEnding::Cancel { result: result() },
    ];
    for ending in endings {
        let mut end = EndRunOperation::new(job(), token(), ending, SCOPE);
        assert!(is_child(&end.first()));
        assert!(is_child(&end.next(stored())));
        assert_eq!(end.next(announced()), Ok(Next::Done(())));
    }
}

// A success reads the stored progress first; without a record it starts a fresh one.
#[test]
fn completion_reads_progress() {
    for found in [Some(record()), None] {
        let ending = RunEnding::Complete { result: result() };
        let mut end = EndRunOperation::new(job(), token(), ending, SCOPE);
        assert!(is_child(&end.first()));
        let read = child(Ok::<_, JobMutationError>(found));
        assert!(is_child(&end.next(read)));
        assert!(is_child(&end.next(stored())));
    }
}

// A write that lost its race announces nothing.
#[test]
fn lost_write_stays_silent() {
    let ending = RunEnding::Cancel { result: result() };
    let mut end = EndRunOperation::new(job(), token(), ending, SCOPE);
    end.first().unwrap();
    let lost = child(Err::<JobRecord, _>(JobMutationError::TokenMismatch));
    assert_eq!(end.next(lost), Ok(Next::Done(())));
}

fn park() -> ParkAttemptOperation {
    let mut park = ParkAttemptOperation::new(job(), token(), JobError::retryable("unclear"), SCOPE);
    assert!(is_child(&park.first()));
    park
}

// A parked attempt waits for the next reconcile pass.
#[test]
fn parked_attempt_ends() {
    let mut operation = park();
    let parked = child(Ok::<_, JobMutationError>(ParkOutcome::Parked(record())));
    assert_eq!(operation.next(parked), Ok(Next::Done(())));
}

// An exhausted attempt is announced as it stands.
#[test]
fn exhausted_attempt_announced() {
    let mut operation = park();
    let exhausted = child(Ok::<_, JobMutationError>(ParkOutcome::Exhausted(record())));
    assert!(is_child(&operation.next(exhausted)));
    assert_eq!(operation.next(announced()), Ok(Next::Done(())));
}

// An exhausted attempt with a cancel request settles as cancelled, also if that write fails.
#[test]
fn exhausted_cancel_settles() {
    let mut cancelling = record();
    cancelling.cancel_requested = true;
    let cancelled = CancelRequestOutcome::Cancelled(record());
    for reply in [
        child(Ok::<_, JobMutationError>(cancelled)),
        child(Err::<CancelRequestOutcome, _>(JobMutationError::NotFound)),
    ] {
        let mut operation = park();
        let exhausted = ParkOutcome::Exhausted(cancelling.clone());
        let marked = child(Ok::<_, JobMutationError>(exhausted));
        assert!(is_child(&operation.next(marked)));
        assert!(is_child(&operation.next(reply)));
        assert_eq!(operation.next(announced()), Ok(Next::Done(())));
    }
}

// A failed park write is only logged.
#[test]
fn park_write_failure_ends() {
    let mut operation = park();
    let failed = child(Err::<ParkOutcome, _>(JobMutationError::NotFound));
    assert_eq!(operation.next(failed), Ok(Next::Done(())));
}

fn failure(error: JobError) -> SubmitFailureOperation {
    SubmitFailureOperation::new(&record(), token(), error, false, SCOPE)
}

// A permanent failure ends the job through its terminal write.
#[test]
fn permanent_failure_ends_job() {
    let mut operation = failure(JobError::permanent("bad image"));
    assert!(is_child(&operation.first()));
    assert_eq!(operation.step, FailureStep::End);
    assert_eq!(operation.next(announced()), Ok(Next::Done(())));
}

// A retryable failure requeues; only a job that ran out of attempts is announced.
#[test]
fn retryable_failure_requeues() {
    let mut queued = failure(JobError::retryable("later"));
    assert!(is_child(&queued.first()));
    assert_eq!(queued.step, FailureStep::Requeue);
    assert_eq!(queued.next(stored()), Ok(Next::Done(())));

    let mut settled = record();
    settled.state = JobState::Failed;
    let mut exhausted = failure(JobError::retryable("later"));
    exhausted.first().unwrap();
    let reply = child(Ok::<_, JobMutationError>(settled));
    assert!(is_child(&exhausted.next(reply)));
    assert_eq!(exhausted.next(announced()), Ok(Next::Done(())));
}

// A bucket is named only when the run has one, and cut logs say so.
#[test]
fn result_records_evidence() {
    let logs = LogTails {
        stdout: b"out".to_vec(),
        stdout_truncated: true,
        ..Default::default()
    };
    let JobResultPayload::Execution {
        workspace_bucket,
        stdout,
        stderr,
        ..
    } = execution_result("", None, Vec::new(), logs)
    else {
        panic!("an execution result");
    };
    assert_eq!(workspace_bucket, None);
    assert_eq!(stdout, "[truncated]\nout");
    assert_eq!(stderr, "");
}
