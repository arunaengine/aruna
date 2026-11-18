//! Tests the calls and results of the launch admission operations without any I/O.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
use aruna_core::effects::{LaunchFrame, ReceiptFrame};
use aruna_core::errors::StorageError;
use aruna_core::events::LaunchDecline;
use aruna_core::operation::Operation;
use aruna_core::scheduling::PlannedInput;
use aruna_core::structs::execution::job::{JobFamilyRecord, JobRecordEnvelope, LaunchIntent};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use ulid::Ulid;

use super::admission_race::{config, envelope};
use crate::document_repository::GetConfigError;
use crate::jobs::lifecycle::LifecycleError;
use crate::jobs::lifecycle::launch_admit::{
    AdmitLaunchOperation, CommitReceiptOperation, LaunchAnswer, LaunchNode, LaunchUndecided,
    RESERVE_ATTEMPTS, SettleLaunchOperation,
};
use crate::jobs::lifecycle::reservation::ReserveExecutionConfig;
use crate::jobs::records::RecordStoreError;
use crate::jobs::records::append::AppendOutcome;
use crate::jobs::records::family::FamilyReadError;
use crate::jobs::records::pages::PageReadError;
use crate::tests::records::{Family, REALM, node};

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn calls_child<T: std::fmt::Debug>(next: Result<Next<T>, LaunchUndecided>) {
    assert!(matches!(next, Ok(Next::Call(IoCall::Child(_)))), "{next:?}");
}

fn failed(error: StorageError) -> IoResult {
    child(Err::<Ulid, _>(LifecycleError::Storage(error)))
}

fn records(found: Vec<JobRecordEnvelope>) -> IoResult {
    child(Ok::<_, FamilyReadError>(found))
}

fn commit() -> (Family, LaunchIntent, ReserveExecutionConfig) {
    let family = Family::new([5; 32]);
    let launch = family.launch(&family.spec(), family.holder.public(), 0);
    let config = config(&family, &launch, 1, envelope(4));
    (family, launch, config)
}

fn receipt_of(config: &ReserveExecutionConfig) -> ReceiptFrame {
    ReceiptFrame::new(config.receipt.envelope().clone()).expect("bounded receipt")
}

// A committed reservation answers with its own receipt, after the cancellation check.
#[test]
fn commit_settles_receipt() {
    let (_, launch, config) = commit();
    let mut operation = CommitReceiptOperation::new(config.clone(), launch);
    calls_child(operation.first());
    calls_child(operation.next(child(Ok::<_, LifecycleError>(Ulid::from_parts(1, 1)))));
    let answer = LaunchAnswer {
        decision: Ok(receipt_of(&config)),
        cancel: None,
    };
    let settled = operation.next(child(Ok::<_, LaunchUndecided>(LaunchAnswer {
        decision: Ok(receipt_of(&config)),
        cancel: None,
    })));
    assert_eq!(settled, Ok(Next::Done(answer)));
}

// Writes storage never accepted are tried a bounded number of times, then stay undecided.
#[test]
fn retries_stay_bounded() {
    let (_, launch, config) = commit();
    let mut operation = CommitReceiptOperation::new(config, launch);
    calls_child(operation.first());
    for _ in 1..RESERVE_ATTEMPTS {
        calls_child(operation.next(failed(StorageError::QueueFull)));
    }
    assert_eq!(
        operation.next(failed(StorageError::QueueFull)),
        Err(LaunchUndecided)
    );
}

// A lost race looks for the winner's receipt and otherwise reserves again.
#[test]
fn lost_race_retries() {
    let (_, launch, config) = commit();
    let mut operation = CommitReceiptOperation::new(config, launch);
    calls_child(operation.first());
    calls_child(operation.next(failed(StorageError::TransactionConflict)));
    calls_child(operation.next(records(Vec::new())));
    calls_child(operation.next(failed(StorageError::TransactionConflict)));
}

// An unknown commit outcome is never reserved again: without a receipt it stays undecided.
#[test]
fn uncertain_never_reserves() {
    let (_, launch, config) = commit();
    let mut operation = CommitReceiptOperation::new(config, launch);
    calls_child(operation.first());
    calls_child(operation.next(failed(StorageError::CommitFailed)));
    assert_eq!(operation.next(records(Vec::new())), Err(LaunchUndecided));
}

// Exact capacity and a lost membership are refusals, a local fault decides nothing.
#[test]
fn verdicts_end_commit() {
    let answer = |error| {
        let (_, launch, config) = commit();
        let mut operation = CommitReceiptOperation::new(config, launch);
        operation.first().expect("the reservation starts");
        operation.next(child(Err::<Ulid, _>(error)))
    };
    let declined = |reason| {
        Ok(Next::Done(LaunchAnswer {
            decision: Err(reason),
            cancel: None,
        }))
    };
    assert_eq!(
        answer(LifecycleError::Capacity),
        declined(LaunchDecline::Capacity)
    );
    assert_eq!(
        answer(LifecycleError::NotHolder),
        declined(LaunchDecline::Draining)
    );
    assert_eq!(answer(LifecycleError::NotFinished), Err(LaunchUndecided));
}

// A refusal needs no read, and an admitted launch of a cancelled family names its run.
#[test]
fn settle_names_cancelled_run() {
    let mut refused = SettleLaunchOperation::new(Err(LaunchDecline::Capacity));
    assert_eq!(
        refused.first(),
        Ok(Next::Done(LaunchAnswer {
            decision: Err(LaunchDecline::Capacity),
            cancel: None,
        }))
    );
    let (family, _, config) = commit();
    let frame = receipt_of(&config);
    let mut settle = SettleLaunchOperation::new(Ok(frame.clone()));
    calls_child(settle.first());
    let cancel = JobFamilyRecord::Cancel(family.cancel(&family.spec()));
    let cancel = family.sign(&family.holder, cancel);
    assert_eq!(
        settle.next(records(vec![cancel])),
        Ok(Next::Done(LaunchAnswer {
            decision: Ok(frame),
            cancel: Some(config.job_id),
        }))
    );
}

// An incomplete family read leaves an admitted offer undecided.
#[test]
fn settle_needs_full_read() {
    let (_, _, config) = commit();
    let mut settle = SettleLaunchOperation::new(Ok(receipt_of(&config)));
    calls_child(settle.first());
    let unread = FamilyReadError::MalformedKey;
    assert_eq!(
        settle.next(child(Err::<Vec<JobRecordEnvelope>, _>(unread))),
        Err(LaunchUndecided)
    );
}

fn admit(family: &Family, launch: &LaunchIntent) -> AdmitLaunchOperation {
    let record = JobFamilyRecord::Launch(Box::new(launch.clone()));
    let frame = LaunchFrame::new(family.sign(&family.holder, record)).expect("bounded launch");
    let node = LaunchNode {
        local: family.target.public(),
        realm_id: REALM,
        now_ms: 4_000,
        execution_id: Ulid::from_parts(9, 9),
        limits: Some(envelope(4)),
    };
    AdmitLaunchOperation::new(frame, node)
}

fn config_answer(family: &Family) -> IoResult {
    child(Ok::<_, GetConfigError>(family.config.clone()))
}

// An offer for another node is refused before any record is read.
#[test]
fn other_target_refused() {
    let family = Family::new([5; 32]);
    let mut launch = family.launch(&family.spec(), family.holder.public(), 0);
    launch.target.node_id = node(9);
    let mut operation = admit(&family, &launch);
    calls_child(operation.first());
    assert_eq!(
        operation.next(config_answer(&family)),
        Ok(Next::Done(Err(LaunchDecline::Unauthorized)))
    );
}

// Without the realm config the offer cannot be decided.
#[test]
fn config_needed() {
    let family = Family::new([5; 32]);
    let launch = family.launch(&family.spec(), family.holder.public(), 0);
    let mut operation = admit(&family, &launch);
    calls_child(operation.first());
    let missing = Err::<RealmConfigDocument, _>(GetConfigError::DocumentNotFound);
    assert_eq!(operation.next(child(missing)), Err(LaunchUndecided));
}

// A launch that stays unretained after the family was pulled leaves the offer undecided.
#[test]
fn unretained_launch_undecided() {
    let family = Family::new([5; 32]);
    let spec = family.spec();
    let launch = family.launch(&spec, family.holder.public(), 0);
    let stored = family.sign(
        &family.holder,
        JobFamilyRecord::Spec(Box::new(spec.clone())),
    );
    let refused = || {
        child(Err::<AppendOutcome, _>(RecordStoreError::Storage(
            StorageError::Timeout,
        )))
    };
    let mut operation = admit(&family, &launch);
    calls_child(operation.first());
    calls_child(operation.next(config_answer(&family)));
    calls_child(operation.next(records(vec![stored])));
    calls_child(operation.next(child(Ok::<_, PageReadError>(spec))));
    let planned = Ok::<_, PageReadError>(Vec::<PlannedInput>::new());
    calls_child(operation.next(child(planned)));
    calls_child(operation.next(refused()));
    calls_child(operation.next(child(Ok::<(), IoError>(()))));
    assert_eq!(operation.next(refused()), Err(LaunchUndecided));
}
