//! Tests the shared plugin row operations and the byte conversions of their calls and answers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

const SPACE: &str = "plugin_state";

#[test]
fn writes_then_deletes() {
    let rows = vec![row(SPACE, b"a".to_vec(), b"1".to_vec())];
    let mut plan = Writes {
        rows: Some(rows.clone()),
        deletes: Some(vec![(SPACE, b"b".to_vec())]),
    };
    assert_eq!(plan.first().unwrap(), Next::Call(IoCall::Write(rows)));
    let deleted = delete_call(vec![(SPACE, b"b".to_vec())]);
    assert_eq!(plan.next(IoResult::Done).unwrap(), Next::Call(deleted));
    assert_eq!(plan.next(IoResult::Done).unwrap(), Next::Done(()));
    assert!(plan.next(values_result(Vec::new())).is_err());
}

#[test]
fn scans_every_page() {
    let mut plan = ScanRows(Scan::new(SPACE, b"p".to_vec()));
    let Next::Call(IoCall::Page { after, limit, .. }) = plan.first().unwrap() else {
        panic!("a scan reads a page");
    };
    assert_eq!((after.map(|key| key.to_vec()), limit), (None, 1_000));
    let first = vec![(b"p1".to_vec(), b"1".to_vec())];
    let Next::Call(IoCall::Page { after, .. }) =
        plan.next(page_result(first, Some(b"p1".to_vec()))).unwrap()
    else {
        panic!("a scan continues after the last key");
    };
    assert_eq!(after.map(|key| key.to_vec()), Some(b"p1".to_vec()));
    let second = vec![(b"p2".to_vec(), b"2".to_vec())];
    let Next::Done(rows) = plan.next(page_result(second, None)).unwrap() else {
        panic!("the last page ends the scan");
    };
    assert_eq!(rows.len(), 2);
    assert!(plan.next(IoResult::Done).is_err());
}

#[test]
fn single_requests() {
    let mut read = ReadRows(Some(read_call(vec![(SPACE, b"a".to_vec())])));
    assert!(matches!(read.first().unwrap(), Next::Call(IoCall::Read(_))));
    assert!(read.first().is_err());
    let stored = vec![Some(b"1".to_vec())];
    let answer = read.next(values_result(stored.clone())).unwrap();
    assert_eq!(answer, Next::Done(stored));
    let mut page = PageRows(Some(page_call(SPACE, Vec::new(), None, 1)));
    page.first().unwrap();
    let answer = page.next(page_result(Vec::new(), None)).unwrap();
    assert_eq!(answer, Next::Done((Vec::new(), None)));
    let mut change = Change(Some(IoCall::Sync));
    assert_eq!(change.first().unwrap(), Next::Call(IoCall::Sync));
    assert_eq!(change.next(IoResult::Done).unwrap(), Next::Done(()));
    assert!(change.first().is_err());
    assert!(values::<2>(values_result(vec![None])).is_err());
}

#[test]
fn modes_and_conflicts() {
    assert_eq!(
        writes(Vec::new(), Vec::new()).transaction(),
        aruna_core::operation::TransactionMode::Write
    );
    assert_eq!(
        sync_all().transaction(),
        aruna_core::operation::TransactionMode::None
    );
    assert_eq!(sync_all().first().unwrap(), Next::Call(IoCall::Sync));
    let conflict = StoreError::from(IoError::Conflict);
    assert!(<Writes as Operation>::is_conflict(&conflict));
    assert!(!<Writes as Operation>::is_conflict(&StoreError(
        "other".into()
    )));
}
