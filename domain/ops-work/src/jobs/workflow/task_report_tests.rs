//! Tests the report keep as calls and results: what it collects, keeps as none or only warns
//! about, and that the write keeps a report only where the work and task rows exist.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::ChildOutcome;
use aruna_core::compute::BackendError;
use aruna_core::compute::effect::{OutputBody, OutputFailure};
use aruna_core::install_work::{WorkKind, index_key};
use aruna_core::stream::BackendStream;

use super::*;
use crate::jobs::lifecycle::screen::tests::install;
use crate::tests::calls::values_of;

fn ids() -> (Ulid, Ulid) {
    (install().install_id, Ulid::from_parts(2, 2))
}

fn started() -> KeepReport {
    let mut keep = KeepReport::new(ids(), IoCall::Read(Vec::new()));
    assert!(matches!(keep.first(), Ok(Next::Call(IoCall::Read(_)))));
    keep
}

fn opened(size: u64) -> IoResult {
    let body = OutputBody {
        size,
        bytes: BackendStream(Box::pin(futures_util::stream::empty())),
        failure: OutputFailure::default(),
    };
    let answer = ComputeAnswer::Output(Ok(body));
    IoResult::Event(Box::new(Event::Compute(Box::new(answer))))
}

fn stored(report: Option<&str>) -> Next<()> {
    let store = StoreReport::new(ids(), report.map(str::to_string));
    Next::Call(IoCall::child(store))
}

fn kept(result: Result<(), IoError>) -> IoResult {
    IoResult::Child(ChildOutcome::new(result))
}

#[test]
fn small_report_collected() {
    let mut keep = started();
    let collect = keep.next(opened(6));
    assert!(matches!(collect, Ok(Next::Call(IoCall::Effect(_)))));
    let bytes = Ok(b"mapped".to_vec());
    let collected = Event::blob(BlobEvent::StreamCollected { bytes });
    let next = keep.next(IoResult::Event(Box::new(collected)));
    assert_eq!(next, Ok(stored(Some("mapped"))));
    assert_eq!(keep.next(kept(Ok(()))), Ok(Next::Done(())));
}

#[test]
fn large_report_skipped() {
    let mut keep = started();
    assert_eq!(keep.next(opened(MAX_REPORT as u64 + 1)), Ok(stored(None)));
    assert_eq!(keep.phase, ReportPhase::Keep);
    let mut missing = started();
    let answer = ComputeAnswer::Output(Err(BackendError::Unavailable("gone".to_string())));
    let event = IoResult::Event(Box::new(Event::Compute(Box::new(answer))));
    assert_eq!(missing.next(event), Ok(stored(None)));
    // A keep that fails leaves the attempt to finish.
    let failed = kept(Err(IoError::Codec("down".to_string())));
    assert_eq!(missing.next(failed), Ok(Next::Done(())));
}

/// The store after it found the work row, with its next call.
fn found(store: &mut StoreReport, row: &WorkRow) -> ReportNext {
    let Ok(Next::Call(IoCall::Read(index))) = store.first() else {
        panic!("the work row is looked up by its index");
    };
    assert_eq!(&*index[0].1, &index_key(ids().0, ids().1)[..]);
    let ask = store.next(values_of(vec![Some(b"row".to_vec())]));
    assert!(matches!(ask, Ok(Next::Call(IoCall::Read(_)))));
    store.next(values_of(vec![Some(encode(row).unwrap().to_vec())]))
}

#[test]
fn store_needs_rows() {
    let row = WorkRow::reserved(&install(), WorkKind::Recipe, (Ulid::nil(), ids().1), 1);
    let mut store = StoreReport::new(ids(), Some("mapped".to_string()));
    assert_eq!(store.transaction(), TransactionMode::Write);
    assert!(matches!(
        found(&mut store, &row),
        Ok(Next::Call(IoCall::Read(_)))
    ));
    let task = encode(&TaskRow::default()).unwrap().to_vec();
    let Ok(Next::Call(IoCall::Write(rows))) = store.next(values_of(vec![Some(task)])) else {
        panic!("an existing task row keeps the report");
    };
    let written: TaskRow = decode(&rows[1].2).unwrap();
    assert_eq!(written.report.as_deref(), Some("mapped"));
    assert_eq!(decode::<WorkRow>(&rows[0].2).unwrap(), row);
    assert_eq!(store.next(IoResult::Done), Ok(Next::Done(())));
    // Without a task row, or without the work row, nothing is written.
    let mut bare = StoreReport::new(ids(), Some("mapped".to_string()));
    found(&mut bare, &row).unwrap();
    assert_eq!(bare.next(values_of(vec![None])), Ok(Next::Done(())));
    let mut gone = StoreReport::new(ids(), None);
    gone.first().unwrap();
    assert_eq!(gone.next(values_of(vec![None])), Ok(Next::Done(())));
}
