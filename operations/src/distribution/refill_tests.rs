//! Tests the sample refill: a lost sample is rebuilt exactly as incremental writes left it,
//! across interrupted runs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::distribution::test_store::{apply, context, counters, install, open, range, write};
use crate::driver::drive;
use aruna_core::calls::ChildOutcome;
use aruna_core::events::{Event, StorageEvent};
use aruna_storage::storage::StorageHandle;

fn set() -> Ulid {
    Ulid::from_bytes([6; 16])
}

fn range_id() -> Ulid {
    Ulid::from_bytes([1; 16])
}

async fn sample(handle: &StorageHandle) -> Vec<(Key, Value)> {
    let event = handle
        .send_storage_effect(StorageEffect::Iter {
            key_space: KEY_SAMPLE_KEYSPACE.to_string(),
            prefix: Some(sample_prefix(set(), range_id()).into()),
            start: None,
            limit: usize::MAX,
            txn_id: None,
        })
        .await;
    let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
        panic!("unexpected storage event: {event:?}");
    };
    values
}

async fn refill(handle: &StorageHandle, max_pages: usize) -> RefillOutcome {
    let config = RefillConfig {
        set_id: set(),
        range_id: range_id(),
        page_rows: 7,
        page_bytes: 1 << 20,
        max_pages,
    };
    drive(RefillOperation::new(config), &context(handle))
        .await
        .expect("refill runs")
}

#[tokio::test]
async fn restores_lost_sample() {
    let (_dir, handle) = open();
    install(&handle, set(), &[range(1, "", None)]).await;
    for seed in 0..60u8 {
        let key = format!("k/{seed:03}");
        assert!(apply(&handle, write(set(), &key, seed, 1)).await.unwrap());
    }
    let written = sample(&handle).await;
    assert!(!written.is_empty());
    let deletes = (written.iter())
        .map(|(key, _)| (KEY_SAMPLE_KEYSPACE.to_string(), key.clone()))
        .collect();
    let event = handle
        .send_storage_effect(StorageEffect::BatchDelete {
            deletes,
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::BatchDeleteResult { .. })
    ));

    assert_eq!(refill(&handle, 2).await, RefillOutcome::Paused);
    assert_eq!(refill(&handle, 100).await, RefillOutcome::Done);
    assert_eq!(
        sample(&handle).await,
        written,
        "a refill places keys as writes did"
    );
    let count = counters(&handle, sample_count_key(set(), range_id())).await;
    assert_eq!(count.sampled, 60, "the refill corrects the count row");
}

/// A sample larger than one read is trimmed over repeated pages, and only a complete read
/// corrects the count.
#[tokio::test]
async fn trims_crowded_sample() {
    use crate::distribution::test_store::crowd_sample;

    let (_dir, handle) = open();
    install(&handle, set(), &[range(1, "", None)]).await;
    crowd_sample(&handle, set(), 1, 1200).await;
    assert_eq!(refill(&handle, 100).await, RefillOutcome::Done);
    assert_eq!(sample(&handle).await.len(), usize::from(SAMPLE_SIZE));
    let count = counters(&handle, sample_count_key(set(), range_id())).await;
    assert_eq!(count.sampled, i128::from(SAMPLE_SIZE));
}

fn config(max_pages: usize) -> RefillConfig {
    RefillConfig {
        set_id: set(),
        range_id: range_id(),
        page_rows: 7,
        page_bytes: 64,
        max_pages,
    }
}

fn span() -> PageSpan {
    PageSpan {
        start: IterStart::At(b"low".to_vec().into()),
        upper: b"up".to_vec(),
        probe_end: Some(b"row".to_vec()),
        terminal: false,
    }
}

fn account_read() -> IoResult {
    let account = RangeAccount::fresh(String::new(), None);
    IoResult::Values(vec![Some(account.to_bytes().unwrap().into())])
}

fn cursor_row(cursor: &Cursor) -> IoResult {
    IoResult::Values(vec![Some(postcard::to_allocvec(cursor).unwrap().into())])
}

fn range_rows(rows: Vec<(Key, Value)>, next: Option<Key>) -> IoResult {
    IoResult::Event(Box::new(Event::Storage(StorageEvent::IterResult {
        values: rows,
        next_start_after: next,
    })))
}

fn page_end(end: Result<PageEnd, RefillError>) -> IoResult {
    IoResult::Child(ChildOutcome::new(end))
}

fn at_page(max_pages: usize) -> RefillOperation {
    let mut run = RefillOperation::new(config(max_pages));
    run.first().unwrap();
    run.next(account_read()).unwrap();
    run.next(IoResult::Values(vec![None])).unwrap();
    assert!(matches!(
        run.next(range_rows(Vec::new(), None)),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    run
}

// A retired range is refused; a missing account is reported.
#[test]
fn refuses_missing_retired() {
    let mut run = RefillOperation::new(config(1));
    run.first().unwrap();
    assert_eq!(
        run.next(IoResult::Values(vec![None])),
        Err(RefillError::RangeMissing(range_id()))
    );
    let retired = RangeAccount {
        charge: Charge::Retired {
            generation: 1,
            left: Ulid::nil(),
            right: Ulid::nil(),
        },
        ..RangeAccount::fresh(String::new(), None)
    };
    let mut run = RefillOperation::new(config(1));
    run.first().unwrap();
    let stored = IoResult::Values(vec![Some(retired.to_bytes().unwrap().into())]);
    assert_eq!(run.next(stored), Err(RefillError::RangeRetired(range_id())));
}

// A committed page continues until the budget or the end of the range.
#[test]
fn pages_until_budget() {
    let mut run = at_page(1);
    let end = page_end(Ok(PageEnd::Committed(Cursor::At(b"row".to_vec()))));
    assert_eq!(run.next(end), Ok(Next::Done(RefillOutcome::Paused)));
    let mut run = at_page(2);
    let end = page_end(Ok(PageEnd::Committed(Cursor::End)));
    assert_eq!(run.next(end), Ok(Next::Done(RefillOutcome::Done)));
}

// A stale page and a lost commit read the cursor again until the budget runs out.
#[test]
fn conflicts_restart_pause() {
    let read = Ok(Next::Call(read_cursor(&config(1))));
    let mut run = at_page(1);
    assert_eq!(run.next(page_end(Ok(PageEnd::Stale))), read);
    let mut run = at_page(1);
    assert_eq!(run.next(page_end(Err(IoError::Conflict.into()))), read);
    let mut run = at_page(1);
    run.conflicts = MAX_CONFLICTS;
    assert_eq!(
        run.next(page_end(Ok(PageEnd::Stale))),
        Ok(Next::Done(RefillOutcome::Paused))
    );
    let mut run = at_page(1);
    let failed = page_end(Err(IoError::Aborted.into()));
    assert_eq!(run.next(failed), Err(IoError::Aborted.into()));
}

// A cursor another pass moved ends the page without a write.
#[test]
fn moved_cursor_stale() {
    let mut page = RefillPageOperation::new(config(1), Cursor::Start, span());
    assert_eq!(page.transaction(), TransactionMode::Write);
    assert_eq!(page.first(), Ok(Next::Call(read_cursor(&config(1)))));
    let moved = cursor_row(&Cursor::At(b"other".to_vec()));
    assert_eq!(page.next(moved), Ok(Next::Done(PageEnd::Stale)));
}

// A page reads up to the probe, merges the sample, writes the cursor and corrects the count.
#[test]
fn page_merges_sample() {
    let mut page = RefillPageOperation::new(config(1), Cursor::Start, span());
    page.first().unwrap();
    let read = page.next(cursor_row(&Cursor::End)).unwrap();
    let expected = span().page_read(&config(1));
    assert_eq!(read, Next::Call(expected));
    let sample = page.next(range_rows(Vec::new(), None)).unwrap();
    assert!(matches!(sample, Next::Call(IoCall::Page { .. })));
    let sampled = IoResult::Page {
        rows: Vec::new(),
        next: None,
    };
    let count = read_row(RANGE_COUNTER_KEYSPACE, sample_count_key(set(), range_id()));
    assert_eq!(page.next(sampled), Ok(Next::Call(count)));
    let counted = RangeCounters {
        sampled: 2,
        ..RangeCounters::default()
    };
    let stored = IoResult::Values(vec![Some(counted.to_bytes().unwrap().into())]);
    let Ok(Next::Call(IoCall::Write(writes))) = page.next(stored) else {
        panic!("expected the cursor row");
    };
    assert_eq!(writes.len(), 1);
    assert_eq!(
        page.next(IoResult::Done),
        Ok(Next::Call(IoCall::Delete(Vec::new())))
    );
    let Ok(Next::Call(IoCall::AddCounters { deltas, .. })) = page.next(IoResult::Done) else {
        panic!("expected the count correction");
    };
    assert_eq!(deltas[0].1.sampled, -2);
    let end = PageEnd::Committed(Cursor::At(b"row".to_vec()));
    assert_eq!(page.next(IoResult::Done), Ok(Next::Done(end)));
}
