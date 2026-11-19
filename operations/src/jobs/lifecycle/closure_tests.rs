//! Tests both closures as calls and results: a submission's closure row, a found claim, an
//! outsider, the expiry and sweep of old rows; a launch's answers, query and holder pass.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::ChildOutcome;
use aruna_core::events::MetadataAnswer;
use aruna_core::structs::execution::job::LAUNCH_WINDOW_MS;

use super::*;
use crate::tests::records::{Family, REALM, node};

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

/// The fixture's signed launch and the family records with its receipt.
fn launch(family: &Family) -> (LaunchFrame, Vec<JobRecordEnvelope>) {
    let run = family.run(
        1,
        0,
        aruna_core::structs::execution::job::PhysicalExecutionState::Failed,
    );
    let launch = run[3].clone();
    let frame = LaunchFrame::new(launch).expect("bounded launch");
    (frame, run)
}

fn created(frame: &LaunchFrame) -> u64 {
    let JobFamilyRecord::Launch(intent) = &frame.envelope().record else {
        panic!("a launch");
    };
    intent.created_at_ms
}

/// The target's answer to `frame` at `now_ms`, with `receipts` stored; without a receipt the
/// family's `tombstone` row is read too.
fn answered(
    family: &Family,
    frame: LaunchFrame,
    now_ms: u64,
    (receipts, tombstone): (Vec<JobRecordEnvelope>, Option<FamilyTombstone>),
) -> Result<Next<LaunchClosureAnswer>, ClosureError> {
    let serving = Some((family.target.public(), REALM));
    let mut answer = AnswerClosureOperation::new(frame, serving, None);
    answer.stamp(now_ms);
    assert!(matches!(answer.first(), Ok(Next::Call(IoCall::Child(_)))));
    let read = answer.next(child(Ok::<_, FamilyReadError>(receipts)));
    let Ok(Next::Call(IoCall::Read(_))) = read else {
        return read;
    };
    let row = tombstone.map(|tombstone| Value::from(to_bytes(&tombstone).unwrap().as_slice()));
    answer.next(IoResult::Values(vec![row]))
}

// The target answers from its rows: its receipt, too early inside the window, never after it.
#[test]
fn target_answers_rows() {
    let family = Family::new([70u8; 32]);
    let (frame, run) = launch(&family);
    let start = created(&frame);
    let receipt = run[4].clone();
    let answer = answered(&family, frame.clone(), start, (vec![receipt], None));
    assert!(matches!(
        answer,
        Ok(Next::Done(LaunchClosureAnswer::Receipt(_)))
    ));
    let early = answered(&family, frame.clone(), start + 1, (Vec::new(), None));
    assert_eq!(early, Ok(Next::Done(LaunchClosureAnswer::TooEarly)));
    let after = start + LAUNCH_WINDOW_MS + 1;
    let late = answered(&family, frame.clone(), after, (Vec::new(), None));
    assert_eq!(late, Ok(Next::Done(LaunchClosureAnswer::NeverAccepted)));
    // A family pruned after the launch was made answers pruned, even inside the window.
    let pruned = FamilyTombstone {
        pruned_at_ms: start + 5,
        last_record_ms: start + 1,
        finished: true,
        created_by: None,
    };
    let answer = answered(&family, frame, start + 1, (Vec::new(), Some(pruned)));
    assert_eq!(answer, Ok(Next::Done(LaunchClosureAnswer::Pruned)));
}

// Another node answers nothing for a launch it is not the target of; an outsider is refused.
#[test]
fn refuses_wrong_asker() {
    let family = Family::new([71u8; 32]);
    let (frame, _) = launch(&family);
    let mut other =
        AnswerClosureOperation::new(frame.clone(), Some((family.holder.public(), REALM)), None);
    assert!(matches!(other.first(), Err(ClosureError::Undecided(_))));
    let serving = Some((family.target.public(), REALM));
    let mut asked = AnswerClosureOperation::new(frame, serving, Some(family.holder.public()));
    assert!(matches!(asked.first(), Ok(Next::Call(IoCall::Child(_)))));
    let empty = RealmConfigDocument::new(REALM, Vec::new(), 1);
    let refused = asked.next(child(Ok::<_, GetConfigError>(empty)));
    assert_eq!(refused, Err(ClosureError::Outsider));
}

// A remote target is asked over the transport; never accepted stores a closure row.
#[test]
fn query_stores_closure() {
    let family = Family::new([72u8; 32]);
    let (frame, _) = launch(&family);
    let serving = (family.holder.public(), REALM);
    let mut close = CloseLaunchOperation::new((family.family(), frame), serving).unwrap();
    close.stamp(9);
    let asked = close.first();
    assert!(matches!(asked, Ok(Next::Call(IoCall::Effect(_)))));
    let reply = MetadataTransportMessage::LaunchClosureAnswered {
        answer: LaunchClosureAnswer::NeverAccepted,
    };
    let answer = MetadataAnswer {
        peer: family.target.public(),
        elapsed: Duration::ZERO,
        result: Ok(encode_message(&reply).unwrap().into()),
    };
    let event = IoResult::Event(Box::new(Event::Net(NetEvent::MetadataAnswer(Box::new(
        answer,
    )))));
    let Ok(Next::Call(IoCall::Write(rows))) = close.next(event) else {
        panic!("the closure row is written");
    };
    assert_eq!(rows[0].0, LAUNCH_CLOSURE_KEYSPACE);
    let row: ClosureRow = from_bytes(&rows[0].2).unwrap();
    assert_eq!(
        (row.kind, row.closed_at_ms),
        (ClosureKind::NeverAccepted, 9)
    );
    assert_eq!(
        close.next(IoResult::Done),
        Ok(Next::Done(ClosureOutcome::Closed))
    );
}

// A launch this node is the target of is answered here; too early leaves it open.
#[test]
fn own_target_local() {
    let family = Family::new([73u8; 32]);
    let (frame, _) = launch(&family);
    let serving = (family.target.public(), REALM);
    let mut close = CloseLaunchOperation::new((family.family(), frame), serving).unwrap();
    assert!(matches!(close.first(), Ok(Next::Call(IoCall::Child(_)))));
    let early = child(Ok::<_, ClosureError>(LaunchClosureAnswer::TooEarly));
    assert_eq!(close.next(early), Ok(Next::Done(ClosureOutcome::Later)));
}

// The pass asks only for open launches past their closure time and counts the closed ones.
#[test]
fn asks_due_launches() {
    let family = Family::new([74u8; 32]);
    let (frame, _) = launch(&family);
    let records = vec![frame.envelope().clone()];
    let serving = Some((family.holder.public(), REALM));
    let mut early = CloseLaunchesOperation::new(family.family(), records.clone(), serving);
    early.stamp(created(&frame));
    early.first().unwrap();
    let none = || IoResult::Page {
        rows: Vec::new(),
        next: None,
    };
    assert_eq!(early.next(none()), Ok(Next::Done(BTreeSet::new())));
    let mut due = CloseLaunchesOperation::new(family.family(), records, serving);
    let JobFamilyRecord::Launch(intent) = &frame.envelope().record else {
        panic!("a launch");
    };
    due.stamp(intent.closure_due());
    due.first().unwrap();
    assert!(matches!(due.next(none()), Ok(Next::Call(IoCall::Child(_)))));
    let closed = child(Ok::<_, LifecycleError>(ClosureOutcome::Closed));
    assert_eq!(
        due.next(closed),
        Ok(Next::Done(BTreeSet::from([intent.launch_id])))
    );
}

fn page(rows: Vec<(Key, Value)>, next: Option<Key>) -> IoResult {
    IoResult::Page { rows, next }
}

#[test]
fn absent_writes_closure() {
    let submission = SubmissionId([4; 32]);
    let mut close = CloseSubmissionOperation::new(submission, 9_000);
    assert!(matches!(close.first(), Ok(Next::Call(IoCall::Page { .. }))));
    let Ok(Next::Call(IoCall::Write(rows))) = close.next(page(Vec::new(), None)) else {
        panic!("no claim writes the closure row");
    };
    assert_eq!(rows[0].1, closure_key(submission));
    assert_eq!(rows[0].2.as_ref(), 9_000u64.to_be_bytes());
    // "Absent" is answered only after the row was written in the transaction.
    assert_eq!(
        close.next(IoResult::Done),
        Ok(Next::Done(ClosureAnswer::Absent))
    );
}

#[test]
fn claim_answers_family() {
    let family = Family::new([4; 32]);
    let spec = family.spec();
    let claim = family.claim(&spec);
    let envelope = family.sign(&family.holder, JobFamilyRecord::Claim(claim));
    let row = (
        Key::from(&b"k"[..]),
        Value::from(to_bytes(&envelope).unwrap().as_slice()),
    );
    let mut close = CloseSubmissionOperation::new(family.submission_id, 9_000);
    close.first().unwrap();
    // A later page is not read once a claim is found, and nothing is written.
    let found = close.next(page(vec![row], Some(Key::from(&b"k"[..]))));
    let expected = ClosureAnswer::Claimed {
        family: envelope.family(),
        job_id: claim.job_id,
    };
    assert_eq!(found, Ok(Next::Done(expected)));
}

#[test]
fn outsider_undecided() {
    let mut serve = ServeClosureOperation::new(REALM, node(9), (SubmissionId([4; 32]), 1));
    assert!(matches!(serve.first(), Ok(Next::Call(IoCall::Child(_)))));
    let config = RealmConfigDocument::default_for_realm(REALM, Vec::new());
    let read = child(Ok::<_, GetConfigError>(config));
    assert_eq!(serve.next(read), Ok(Next::Done(ClosureAnswer::Undecided)));
}

#[test]
fn rows_expire_late() {
    let admit_by = 1_000u64.to_be_bytes();
    assert!(!closure_expired(&admit_by, 1_000 + CLOCK_MARGIN_MS));
    assert!(closure_expired(&admit_by, 1_001 + CLOCK_MARGIN_MS));
    // A row that is no deadline never goes.
    assert!(!closure_expired(b"short", u64::MAX));
}

#[test]
fn sweep_drops_expired() {
    let now = 5_000 + CLOCK_MARGIN_MS;
    let old = (
        Key::from(&b"a"[..]),
        Value::from(1_000u64.to_be_bytes().as_slice()),
    );
    let open = (
        Key::from(&b"b"[..]),
        Value::from(9_000u64.to_be_bytes().as_slice()),
    );
    let mut sweep = SweepClosuresOperation::new(now);
    assert!(matches!(sweep.first(), Ok(Next::Call(IoCall::Page { .. }))));
    let Ok(Next::Call(IoCall::Delete(keys))) = sweep.next(page(vec![old, open], None)) else {
        panic!("the expired row is deleted");
    };
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0].1, Key::from(&b"a"[..]));
    assert_eq!(sweep.next(IoResult::Done), Ok(Next::Done(1)));
}
