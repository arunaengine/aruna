//! Reads and writes the import report rows through their calls and answers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::super::tests::{entry, plan, run};
use super::*;
use crate::jobs::repository::test_support::child;

fn no_row() -> IoResult {
    IoResult::Values(vec![None])
}

fn saved() -> IoResult {
    child(Ok::<_, JobMutationError>(()))
}

fn row_bytes(row: &ImportReportRow) -> ByteView {
    ByteView::from(postcard::to_allocvec(row).unwrap())
}

// A missing row reads as none, a stored one decodes, and a broken one retries.
#[test]
fn report_decodes_row() {
    let row = entry_report(&entry("a"));
    assert_eq!(report_of(no_row()).unwrap(), None);
    let stored = IoResult::Values(vec![Some(row_bytes(&row))]);
    assert_eq!(report_of(stored).unwrap(), Some(row));
    let broken = IoResult::Values(vec![Some(ByteView::from(vec![0xFF]))]);
    assert!(matches!(
        report_of(broken),
        Err(ImportFailure::Retryable(_))
    ));
}

// Every page is read, and system rows are left out of the entry reports.
#[test]
fn load_skips_system() {
    let row = entry_report(&entry("a"));
    let mut load = LoadReportsOperation::new(run());
    assert!(matches!(load.first(), Ok(Next::Call(IoCall::Child(_)))));
    let system = (vec![SYSTEM_ENTRY_PREFIX, b'x'], row_bytes(&row));
    let page: row_calls::EntryPage = (vec![system], Some(b"a".to_vec()));
    let more = load.next(child(Ok::<_, JobMutationError>(page)));
    assert!(matches!(more, Ok(Next::Call(IoCall::Child(_)))));
    let last: row_calls::EntryPage = (vec![(b"a".to_vec(), row_bytes(&row))], None);
    let Ok(Next::Done(rows)) = load.next(child(Ok::<_, JobMutationError>(last))) else {
        panic!("expected the loaded rows");
    };
    assert_eq!(rows, HashMap::from([("a".to_string(), row)]));
}

// The failed entry keeps the message; the ones after it were not attempted.
#[test]
fn mark_names_failure() {
    let plan = plan(&["a", "b", "c"]);
    let failed = Unreached::WriteFailed("broken".to_string());
    let mut mark = MarkEntriesOperation::new(run(), Some(&plan), 1, failed);
    assert_eq!(mark.first(), Ok(Next::Call(report_read(&run(), "b"))));
    let mut row = entry_report(&entry("b"));
    row.code = ReasonCode::Failed;
    row.message = Some("broken".to_string());
    let write = report_write(&run(), &row).unwrap();
    assert_eq!(mark.next(no_row()), Ok(Next::Call(write)));
    assert_eq!(mark.next(saved()), Ok(Next::Call(report_read(&run(), "c"))));
    let mut row = entry_report(&entry("c"));
    row.code = ReasonCode::NotAttempted;
    row.message = Some("not attempted after an earlier import failure".to_string());
    let write = report_write(&run(), &row).unwrap();
    assert_eq!(mark.next(no_row()), Ok(Next::Call(write)));
    assert_eq!(mark.next(saved()), Ok(Next::Done(())));
}

// A cancelled import marks all open entries alike, and no plan means nothing to mark.
#[test]
fn mark_after_cancel() {
    let plan = plan(&["a"]);
    let mut mark = MarkEntriesOperation::new(run(), Some(&plan), 0, Unreached::Cancelled);
    mark.first().unwrap();
    let mut row = entry_report(&entry("a"));
    row.code = ReasonCode::NotAttempted;
    row.message = Some("not attempted because the import was cancelled".to_string());
    let write = report_write(&run(), &row).unwrap();
    assert_eq!(mark.next(no_row()), Ok(Next::Call(write)));
    let mut none = MarkEntriesOperation::new(run(), None, 0, Unreached::Cancelled);
    assert_eq!(none.first(), Ok(Next::Done(())));
}

// System rows are written one after the other under their own keys.
#[test]
fn system_rows_write() {
    let row = phase_row(ImportPhase::Write, "broken");
    assert_eq!(row.entry_key, "failure/write");
    let mut rows = SystemRowsOperation::new(run(), vec![row.clone()]);
    let write = system_write(&run(), &row).unwrap();
    assert_eq!(rows.first(), Ok(Next::Call(write)));
    assert_eq!(rows.next(saved()), Ok(Next::Done(())));
}
