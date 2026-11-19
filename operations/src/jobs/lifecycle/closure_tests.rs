//! Tests the closure answer as calls and results: absent with a written row, a found claim, a
//! question from a node outside the realm, the expiry and the sweep of old rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::ChildOutcome;
use aruna_core::structs::execution::job::record_rows::to_bytes;

use super::*;
use crate::tests::records::{Family, REALM, node};

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
    let read = IoResult::Child(ChildOutcome::new(Ok::<_, GetConfigError>(config)));
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
