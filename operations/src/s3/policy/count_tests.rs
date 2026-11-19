//! Tests the start of a saved coverage count, how pages add up in it and how a run advances it,
//! as calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::ChildOutcome;
use ulid::Ulid;

use super::*;
use crate::s3::policy::coverage::{AttachmentGap, CopyCounts, CoverageGap};

fn report(generation: u64, observed: usize, gaps: usize, cursor: Option<&[u8]>) -> CoverageReport {
    let gap = CoverageGap {
        key: "object".to_string(),
        version_id: Ulid::from_bytes([9u8; 16]),
        attachment: AttachmentGap::Missing,
        copy: None,
    };
    CoverageReport {
        bucket: "photos".to_string(),
        scope: CoverageScope::CurrentHeads,
        generation,
        target_refs: Vec::new(),
        observed,
        deleted: 1,
        gaps: vec![gap; gaps],
        copies: CopyCounts {
            registered: 2,
            quarantined: 0,
            absent: 0,
            reference_only: observed - 3,
        },
        cursor: cursor.map(|cursor| cursor.to_vec().into()),
        complete: cursor.is_none(),
        limits: Vec::new(),
        elsewhere: 0,
    }
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn value(bytes: Option<Vec<u8>>) -> IoResult {
    IoResult::Values(vec![bytes.map(Into::into)])
}

#[test]
fn start_queues_count() {
    let mut start = StartCountOperation::new("photos".to_string(), 7);
    assert_eq!(start.transaction(), TransactionMode::Write);
    assert!(matches!(start.first(), Ok(Next::Call(IoCall::Read(_)))));
    let Ok(Next::Call(IoCall::Write(rows))) = start.next(value(Some(b"bucket".to_vec()))) else {
        panic!("the record and the queue entry are written");
    };
    let spaces: Vec<&str> = rows.iter().map(|(space, _, _)| space.as_str()).collect();
    assert_eq!(spaces, [COVERAGE_COUNT_KEYSPACE, COUNT_QUEUE_KEYSPACE]);
    let written = CoverageCount::from_bytes(rows[0].2.as_ref()).unwrap();
    assert_eq!(written, CoverageCount::new(7));
    // The migration task wakes only after the commit.
    let Ok(Next::Then { output, calls }) = start.next(IoResult::Done) else {
        panic!("the count answers with the wake");
    };
    assert_eq!((output, calls.len()), (CoverageCount::new(7), 1));
}

#[test]
fn missing_bucket_refused() {
    let mut start = StartCountOperation::new("photos".to_string(), 7);
    start.first().unwrap();
    assert_eq!(start.next(value(None)), Err(StartCountError::NoSuchBucket));
}

#[test]
fn pages_add_up() {
    let first = add_page(CoverageCount::new(7), &report(3, 10, 2, Some(b"k")), 20);
    assert_eq!(first.cursor.as_deref(), Some(&b"k"[..]));
    assert_eq!(first.finished_at_ms, None);
    let last = add_page(first, &report(3, 5, 1, None), 20);
    assert_eq!(
        (last.generation, last.observed, last.deleted, last.gaps),
        (3, 15, 2, 3)
    );
    assert_eq!((last.registered, last.reference_only), (4, 9));
    assert_eq!((last.cursor, last.finished_at_ms), (None, Some(20)));
}

#[test]
fn changed_rules_restart() {
    let first = add_page(CoverageCount::new(7), &report(3, 10, 2, Some(b"k")), 20);
    let restarted = add_page(first, &report(4, 10, 0, Some(b"m")), 20);
    assert_eq!(restarted, CoverageCount::new(7));
}

#[test]
fn pages_stop_early() {
    let mut pages = CountPagesOperation::new("photos".to_string(), CoverageCount::new(7));
    pages.stamp(20);
    assert!(matches!(pages.first(), Ok(Next::Call(IoCall::Child(_)))));
    for _ in 1..COUNT_PAGES {
        let more = child(Ok::<_, CoverageError>(report(3, 10, 0, Some(b"k"))));
        pages.next(more).unwrap();
        assert!(!pages.stored);
    }
    // The last page of the run stores the progress, which leaves more to count.
    let more = child(Ok::<_, CoverageError>(report(3, 10, 0, Some(b"k"))));
    pages.next(more).unwrap();
    assert!(pages.stored);
    assert_eq!(
        pages.next(child(Ok::<_, MigrationError>(()))),
        Ok(Next::Done(true))
    );
}

#[test]
fn deleted_bucket_finishes() {
    let mut pages = CountPagesOperation::new("photos".to_string(), CoverageCount::new(7));
    pages.stamp(20);
    pages.first().unwrap();
    let gone = child(Err::<CoverageReport, _>(CoverageError::NoSuchBucket));
    pages.next(gone).unwrap();
    assert_eq!(pages.record.finished_at_ms, Some(20));
    assert_eq!(
        pages.next(child(Ok::<_, MigrationError>(()))),
        Ok(Next::Done(false))
    );
}

#[test]
fn run_skips_finished() {
    let mut run = ProcessCountsOperation::new();
    assert!(matches!(run.first(), Ok(Next::Call(IoCall::Page { .. }))));
    let queued = IoResult::Page {
        rows: vec![(Key::from(b"photos".to_vec()), Value::from(Vec::new()))],
        next: None,
    };
    assert!(matches!(run.next(queued), Ok(Next::Call(IoCall::Read(_)))));
    let mut finished = CoverageCount::new(7);
    finished.finished_at_ms = Some(9);
    let read = value(Some(finished.to_bytes().unwrap()));
    assert_eq!(run.next(read), Ok(Next::Done(None)));
}

#[test]
fn stale_store_skipped() {
    // A newer count replaced the record, so the old progress is dropped.
    let mut store = StoreCountOperation::new("photos", CoverageCount::new(7));
    store.first().unwrap();
    let newer = value(Some(CoverageCount::new(8).to_bytes().unwrap()));
    assert_eq!(store.next(newer), Ok(Next::Done(())));
}
