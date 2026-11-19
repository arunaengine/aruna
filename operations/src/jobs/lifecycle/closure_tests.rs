//! Tests the launch closure answers, the query and the holder pass by their calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::ChildOutcome;
use aruna_core::structs::execution::job::LAUNCH_WINDOW_MS;

use super::*;
use crate::tests::records::{Family, REALM};

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

/// The target's answer to `frame` at `now_ms`, with `receipts` stored.
fn answered(
    family: &Family,
    frame: LaunchFrame,
    now_ms: u64,
    receipts: Vec<JobRecordEnvelope>,
) -> Result<Next<ClosureAnswer>, ClosureError> {
    let serving = Some((family.target.public(), REALM));
    let mut answer = AnswerClosureOperation::new(frame, serving, None);
    answer.stamp(now_ms);
    assert!(matches!(answer.first(), Ok(Next::Call(IoCall::Child(_)))));
    answer.next(child(Ok::<_, FamilyReadError>(receipts)))
}

// The target answers from its rows: its receipt, too early inside the window, never after it.
#[test]
fn target_answers_rows() {
    let family = Family::new([70u8; 32]);
    let (frame, run) = launch(&family);
    let start = created(&frame);
    let receipt = run[4].clone();
    let answer = answered(&family, frame.clone(), start, vec![receipt]);
    assert!(matches!(answer, Ok(Next::Done(ClosureAnswer::Receipt(_)))));
    let early = answered(&family, frame.clone(), start + 1, Vec::new());
    assert_eq!(early, Ok(Next::Done(ClosureAnswer::TooEarly)));
    let late = answered(&family, frame, start + LAUNCH_WINDOW_MS + 1, Vec::new());
    assert_eq!(late, Ok(Next::Done(ClosureAnswer::NeverAccepted)));
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
    let reply = MetadataTransportMessage::ForwardedClosureQuery {
        answer: ClosureAnswer::NeverAccepted,
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
    let early = child(Ok::<_, ClosureError>(ClosureAnswer::TooEarly));
    assert_eq!(close.next(early), Ok(Next::Done(ClosureOutcome::Later)));
}

// The pass asks only for open launches past their closure time and counts the closed ones.
#[test]
fn pass_asks_due_launches() {
    let family = Family::new([74u8; 32]);
    let (frame, _) = launch(&family);
    let records = vec![frame.envelope().clone()];
    let serving = (family.holder.public(), REALM);
    let mut early = CloseLaunchesOperation::new(family.family(), records.clone(), serving);
    early.stamp(created(&frame));
    early.first().unwrap();
    let none = || IoResult::Page {
        rows: Vec::new(),
        next: None,
    };
    assert_eq!(early.next(none()), Ok(Next::Done(0)));
    let mut due = CloseLaunchesOperation::new(family.family(), records, serving);
    let JobFamilyRecord::Launch(intent) = &frame.envelope().record else {
        panic!("a launch");
    };
    due.stamp(intent.closure_due());
    due.first().unwrap();
    assert!(matches!(due.next(none()), Ok(Next::Call(IoCall::Child(_)))));
    let closed = child(Ok::<_, LifecycleError>(ClosureOutcome::Closed));
    assert_eq!(due.next(closed), Ok(Next::Done(1)));
}
