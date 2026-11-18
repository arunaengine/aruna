//! Tests the job runtime's decisions by their calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::ChildOutcome;
use aruna_core::structs::execution::job::{JobPayload, lease_index_key};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::Value;

use super::*;

const NOW_MS: u64 = 5_000;

fn job() -> JobId {
    JobId::from_bytes([1; 16])
}

fn token() -> Ulid {
    Ulid::from_bytes([2; 16])
}

fn key() -> BucketKeyRef {
    BucketKeyRef {
        bucket_id: Ulid::from_bytes([3; 16]),
        generation: 1,
    }
}

fn wait() -> KeyWait {
    KeyWait {
        node_id: iroh::SecretKey::from_bytes(&[4; 32]).public(),
        bucket: "data".to_string(),
        group_id: None,
        key: key(),
    }
}

fn record(class: JobExecutionClass, claimed: bool) -> JobRecord {
    let node_id = iroh::SecretKey::from_bytes(&[4; 32]).public();
    let payload = JobPayload::WriteRunCrate { for_job: job() };
    let user = UserId::local(Ulid::from_bytes([5; 16]), RealmId([1; 32]));
    let mut record = JobRecord::new(job(), payload, user, node_id, 1, 1, None);
    record.execution_class = class;
    record.claim = claimed.then_some(aruna_core::structs::execution::job::JobClaim {
        holder_node_id: node_id,
        claim_token: token(),
        lease_expires_ms: 1,
    });
    record
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn written() -> IoResult {
    let record = record(JobExecutionClass::InProcess, false);
    child(Ok::<_, JobMutationError>(record))
}

fn write_failed(error: JobMutationError) -> IoResult {
    child(Err::<JobRecord, _>(error))
}

fn is_child<T>(next: &Result<Next<T>, JobMutationError>) -> bool {
    matches!(next, Ok(Next::Call(IoCall::Child(_))))
}

fn settle(outcome: JobRunOutcome, checkpointed: bool) -> SettleRunOperation {
    let progress = JobProgress::new("steps");
    SettleRunOperation::new((job(), token()), outcome, (progress, checkpointed), NOW_MS)
}

// A storage failure pauses for a growing time and writes again, up to the limit.
#[test]
fn terminal_retries_storage() {
    let mut write = TerminalWriteOperation::new(job(), token(), TerminalWrite::Cancel, NOW_MS);
    assert!(is_child(&write.first()));
    let failure = || write_failed(JobMutationError::Storage("down".to_string()));
    for pause in [20, 40, 80, 160] {
        assert_eq!(
            write.next(failure()),
            Ok(Next::Call(IoCall::Delay(Duration::from_millis(pause))))
        );
        assert!(is_child(&write.next(IoResult::Woke)));
    }
    assert_eq!(
        write.next(failure()),
        Err(JobMutationError::Storage("down".to_string()))
    );
}

// A lost claim is no storage failure and ends the write at once.
#[test]
fn token_loss_ends() {
    let mut write = TerminalWriteOperation::new(job(), token(), TerminalWrite::Cancel, NOW_MS);
    write.first().unwrap();
    assert_eq!(
        write.next(write_failed(JobMutationError::TokenMismatch)),
        Err(JobMutationError::TokenMismatch)
    );
}

// A success writes its terminal state and owes no cleanup handler.
#[test]
fn success_skips_cleanup() {
    let outcome = JobRunOutcome::Succeeded(JobResultPayload::Cleanup);
    let mut run = settle(outcome, false);
    assert!(is_child(&run.first()));
    assert_eq!(run.next(written()), Ok(Next::Done(false)));
}

// A cancelled run owes its cleanup, unless another execution owns the job by now.
#[test]
fn cancel_runs_cleanup() {
    let mut run = settle(JobRunOutcome::Cancelled, false);
    run.first().unwrap();
    assert_eq!(run.next(written()), Ok(Next::Done(true)));

    let mut lost = settle(JobRunOutcome::Cancelled, false);
    lost.first().unwrap();
    let reply = write_failed(JobMutationError::TokenMismatch);
    assert_eq!(lost.next(reply), Ok(Next::Done(false)));

    let mut failed = settle(JobRunOutcome::Cancelled, false);
    failed.first().unwrap();
    let reply = write_failed(JobMutationError::NotFound);
    assert_eq!(failed.next(reply), Ok(Next::Done(true)));
}

// Every other outcome makes one store call and leaves no cleanup, even when it fails.
#[test]
fn other_outcomes_settle() {
    let retryable = || JobError::retryable("again");
    let requeue = || child(Err::<RequeueOutcome, _>(JobMutationError::TokenMismatch));
    let release = || child(Err::<ReleaseOutcome, _>(JobMutationError::NotFound));
    let cases: Vec<(JobRunOutcome, IoResult)> = vec![
        (JobRunOutcome::Failed(retryable()), requeue()),
        (JobRunOutcome::Failed(JobError::permanent("no")), written()),
        (JobRunOutcome::Deferred(retryable()), release()),
        (
            JobRunOutcome::DeferredFor(retryable(), Duration::from_secs(1)),
            release(),
        ),
        (JobRunOutcome::Interrupted, release()),
        (JobRunOutcome::Undecided(retryable()), written()),
    ];
    for (outcome, reply) in cases {
        let mut run = settle(outcome, true);
        assert!(is_child(&run.first()));
        assert_eq!(run.next(reply), Ok(Next::Done(false)));
    }
}

// A job waiting for keys passes on whether its wait ended in a cancel.
#[test]
fn key_wait_reports() {
    let mut run = settle(JobRunOutcome::AwaitingKey(vec![wait()]), false);
    assert!(is_child(&run.first()));
    let reply = child(Ok::<_, JobMutationError>(true));
    assert_eq!(run.next(reply), Ok(Next::Done(true)));
}

fn await_keys() -> AwaitKeysOperation {
    let mut keys = AwaitKeysOperation::new(job(), token(), vec![wait()], NOW_MS);
    assert!(is_child(&keys.first()));
    keys
}

fn parked() -> IoResult {
    let record = record(JobExecutionClass::InProcess, false);
    child(Ok::<_, JobMutationError>(AwaitOutcome::Parked(record)))
}

// A key unlocked while the job parked wakes it here; no unlock call would.
#[test]
fn unlocked_key_wakes() {
    let mut keys = await_keys();
    assert!(is_child(&keys.next(parked())));
    let unlocked = child(Ok::<_, IoError>(true));
    assert!(is_child(&keys.next(unlocked)));
    let woken = child(Ok::<_, JobMutationError>(true));
    assert_eq!(keys.next(woken), Ok(Next::Done(false)));
}

// A key still locked, or one whose state cannot be read, leaves the job parked.
#[test]
fn locked_key_parks() {
    for reply in [
        child(Ok::<_, IoError>(false)),
        child(Err::<bool, _>(IoError::Unfinished)),
    ] {
        let mut keys = await_keys();
        keys.next(parked()).unwrap();
        assert_eq!(keys.next(reply), Ok(Next::Done(false)));
    }
}

// A job that cannot park because a cancel came first is cancelled, with its cleanup.
#[test]
fn skipped_park_cancels() {
    let mut keys = await_keys();
    let skipped = child(Ok::<_, JobMutationError>(AwaitOutcome::Skipped));
    assert!(is_child(&keys.next(skipped)));
    assert_eq!(keys.next(written()), Ok(Next::Done(true)));
}

// A lost claim ends the wait; another park failure defers the job.
#[test]
fn park_failures() {
    let failed = |error| child(Err::<AwaitOutcome, _>(error));
    let mut lost = await_keys();
    let reply = failed(JobMutationError::TokenMismatch);
    assert_eq!(lost.next(reply), Ok(Next::Done(false)));

    let mut deferred = await_keys();
    assert!(is_child(&deferred.next(failed(JobMutationError::NotFound))));
    let reply = child(Ok::<_, JobMutationError>(ReleaseOutcome::Skipped));
    assert_eq!(deferred.next(reply), Ok(Next::Done(false)));
}

// A page names its jobs, skips rows that are no schedule keys and keeps the cursor.
#[test]
fn lease_page_jobs() {
    let mut page = LeasePageOperation::new(None);
    assert_eq!(
        page.first(),
        Ok(Next::Call(IoCall::Page {
            space: SCHEDULE_INDEX_KEYSPACE.to_string(),
            prefix: Key::from(LEASE_INDEX_PREFIX.to_vec()),
            after: None,
            limit: DRAIN_BATCH_SIZE,
        }))
    );
    let row = Key::from(lease_index_key(9, job()).to_vec());
    let rows = vec![
        (row.clone(), Value::from(Vec::new())),
        (Key::from(b"other".to_vec()), Value::from(Vec::new())),
    ];
    let next = Some(row);
    let reply = IoResult::Page {
        rows,
        next: next.clone(),
    };
    assert_eq!(page.next(reply), Ok(Next::Done((vec![job()], next))));
}

fn recover(
    found: Option<JobRecord>,
) -> (
    RecoverJobOperation,
    Result<Next<Recovery>, JobMutationError>,
) {
    let mut recover = RecoverJobOperation::new(job(), NOW_MS);
    assert!(is_child(&recover.first()));
    let next = recover.next(child(Ok::<_, JobMutationError>(found)));
    (recover, next)
}

// A missing job, or an in-process job without a claim, is left alone.
#[test]
fn recovery_skips_unclaimed() {
    assert_eq!(recover(None).1, Ok(Next::Done(Recovery::Untouched)));
    let unclaimed = record(JobExecutionClass::InProcess, false);
    assert_eq!(
        recover(Some(unclaimed)).1,
        Ok(Next::Done(Recovery::Untouched))
    );
}

// An in-process job is released without spending an attempt.
#[test]
fn recovery_releases_local() {
    let claimed = record(JobExecutionClass::InProcess, true);
    let (mut run, next) = recover(Some(claimed.clone()));
    assert!(is_child(&next));
    let reply = child(Ok::<_, JobMutationError>(ReleaseOutcome::Released(claimed)));
    assert_eq!(run.next(reply), Ok(Next::Done(Recovery::Recovered)));

    let (mut lost, _) = recover(Some(record(JobExecutionClass::InProcess, true)));
    let reply = child(Err::<ReleaseOutcome, _>(JobMutationError::TokenMismatch));
    assert_eq!(lost.next(reply), Ok(Next::Done(Recovery::Untouched)));
}

// A submitted external attempt goes to the reconciler and is never queued again.
#[test]
fn recovery_routes_external() {
    let attempt = record(JobExecutionClass::ExternalAttempt, true);
    let (mut run, next) = recover(Some(attempt.clone()));
    assert!(is_child(&next));
    let outcome = RequeueOutcome::NeedsReconcile(attempt.clone());
    assert_eq!(
        run.next(child(Ok::<_, JobMutationError>(outcome))),
        Ok(Next::Done(Recovery::Reconcile(Box::new(attempt))))
    );
}

// A store failure stops the recovery; a job that vanished does not.
#[test]
fn recovery_store_failures() {
    let external = || record(JobExecutionClass::ExternalAttempt, true);
    let (mut gone, _) = recover(Some(external()));
    let reply = child(Err::<RequeueOutcome, _>(JobMutationError::NotFound));
    assert_eq!(gone.next(reply), Ok(Next::Done(Recovery::Untouched)));

    let (mut failed, _) = recover(Some(external()));
    let error = || JobMutationError::Storage("down".to_string());
    let reply = child(Err::<RequeueOutcome, _>(error()));
    assert_eq!(failed.next(reply), Err(error()));
}
