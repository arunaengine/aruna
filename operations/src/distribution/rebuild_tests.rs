//! Tests the counter rebuild against real storage: drift repair, resume from the durable
//! cursor, conflicts with writers that read an old cursor, and empty ranges.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::distribution::inventory::{InventoryChange, InventoryUpdate};
use crate::distribution::test_store::{
    account, apply, begin, commit, context, counters, exact, get, install, open, purge, range,
    stage, write,
};
use crate::driver::drive;
use aruna_core::calls::ChildOutcome;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::structs::storage::distribution::OriginalEpoch;
use aruna_storage::storage::StorageHandle;

fn set() -> Ulid {
    Ulid::from_bytes([4; 16])
}

fn range_id() -> Ulid {
    Ulid::from_bytes([1; 16])
}

fn config(page_rows: usize, max_pages: usize) -> RebuildConfig {
    RebuildConfig {
        set_id: set(),
        range_id: range_id(),
        page_rows,
        page_bytes: 1 << 20,
        max_pages,
        group_id: Some(group()),
    }
}

fn group() -> Ulid {
    Ulid::from_bytes([31; 16])
}

async fn run(handle: &StorageHandle, page_rows: usize, max_pages: usize) -> RebuildOutcome {
    let operation = RebuildOperation::new(config(page_rows, max_pages));
    drive(operation, &context(handle))
        .await
        .expect("rebuild runs")
}

async fn generation(handle: &StorageHandle, generation: u64) -> RangeCounters {
    counters(handle, generation_key(set(), range_id(), generation)).await
}

#[tokio::test]
async fn repairs_drift() {
    let (_dir, handle) = open();
    install(&handle, set(), &[range(1, "", None)]).await;
    for (key, seed, size) in [("a", 1, 10), ("b", 2, 5), ("c", 3, 3)] {
        assert!(apply(&handle, write(set(), key, seed, size)).await.unwrap());
    }
    let txn_id = begin(&handle).await;
    let drift = StorageEffect::AddCounters {
        key_space: RANGE_COUNTER_KEYSPACE.to_string(),
        deltas: vec![(generation_key(set(), range_id(), 1).into(), exact(5, 99))],
        txn_id,
    };
    handle.send_storage_effect(drift).await;
    assert!(commit(&handle, txn_id).await);

    assert_eq!(run(&handle, 2, 100).await, RebuildOutcome::Done);
    let account = account(&handle, set(), range_id()).await.unwrap();
    assert_eq!((account.charge, account.rebuild), (Charge::Active(2), None));
    assert_eq!(generation(&handle, 2).await, exact(3, 18));
    assert!(
        generation(&handle, 1).await.is_zero(),
        "the old generation row is gone"
    );
    // The correction lowers what the range counts: its group's share is marked in the same commit.
    let marker = get(&handle, NODE_STATS_KEYSPACE, dirty_share_key(group())).await;
    assert!(marker.is_some());
}

/// A run that stops after one page resumes from the durable cursor, and writes on both
/// sides of the cursor in between are counted exactly once.
#[tokio::test]
async fn resumes_from_cursor() {
    let (_dir, handle) = open();
    install(&handle, set(), &[range(1, "", None)]).await;
    for seed in 0..10u8 {
        let key = format!("k{seed}");
        assert!(
            apply(&handle, write(set(), &key, seed + 10, 1))
                .await
                .unwrap()
        );
    }
    assert_eq!(run(&handle, 3, 1).await, RebuildOutcome::Paused);
    let paused = account(&handle, set(), range_id()).await.unwrap();
    let rebuild = paused.rebuild.clone().unwrap();
    assert!(matches!(rebuild.versions, Cursor::At(_)) && rebuild.generation == 2);

    assert!(
        apply(&handle, write(set(), "a", 1, 100)).await.unwrap(),
        "behind the cursor"
    );
    assert!(
        apply(&handle, write(set(), "z", 2, 1000)).await.unwrap(),
        "ahead of the cursor"
    );
    assert!(
        apply(&handle, purge(set(), "k0", &[10])).await.unwrap(),
        "behind the cursor"
    );

    assert_eq!(run(&handle, 3, 1).await, RebuildOutcome::Paused);
    assert_eq!(run(&handle, 3, 100).await, RebuildOutcome::Done);
    let account = account(&handle, set(), range_id()).await.unwrap();
    assert_eq!(
        account.charge,
        Charge::Active(2),
        "a resumed run begins no new generation"
    );
    assert_eq!(generation(&handle, 2).await, exact(11, 1109));
}

/// A writer that read the cursor before a page commit conflicts and retries against the
/// new cursor, so its delta lands exactly once.
#[tokio::test]
async fn page_conflicts_writer() {
    let (_dir, handle) = open();
    install(&handle, set(), &[range(1, "", None)]).await;
    for (key, seed) in [("b", 1), ("c", 2), ("d", 3), ("e", 4)] {
        assert!(apply(&handle, write(set(), key, seed, 1)).await.unwrap());
    }
    assert_eq!(run(&handle, 2, 1).await, RebuildOutcome::Paused);
    let writer = begin(&handle).await;
    stage(&handle, writer, &mut write(set(), "a", 5, 10))
        .await
        .unwrap();
    assert_eq!(run(&handle, 2, 1).await, RebuildOutcome::Paused);
    assert!(
        !commit(&handle, writer).await,
        "the page commit changed the cursor"
    );
    assert!(apply(&handle, write(set(), "a", 5, 10)).await.unwrap());

    assert_eq!(run(&handle, 2, 100).await, RebuildOutcome::Done);
    assert_eq!(generation(&handle, 2).await, exact(5, 14));
}

/// An empty range reaches the end with its first page; open uploads are walked too.
#[tokio::test]
async fn walks_empty_uploads() {
    let (_dir, handle) = open();
    install(&handle, set(), &[range(1, "", None)]).await;
    assert_eq!(run(&handle, 2, 100).await, RebuildOutcome::Done);
    assert!(generation(&handle, 2).await.is_zero());

    for upload in 1..=3u8 {
        let mut change = InventoryChange::new(set(), "k");
        change.upload = Some((Ulid::from_bytes([upload; 16]), true));
        assert!(apply(&handle, InventoryUpdate::new(change)).await.unwrap());
    }
    assert_eq!(run(&handle, 2, 100).await, RebuildOutcome::Done);
    assert_eq!(generation(&handle, 3).await.open_uploads, 3);
}

/// A rebuild keeps the epoch maximum of versions that are gone, so the durable maximum never
/// falls below an epoch the range has seen.
#[tokio::test]
async fn keeps_epoch_maximum() {
    let (_dir, handle) = open();
    install(&handle, set(), &[range(1, "", None)]).await;
    let mut change = write(set(), "a", 1, 1).change().clone();
    change.added[0].epoch = OriginalEpoch(7);
    assert!(apply(&handle, InventoryUpdate::new(change)).await.unwrap());
    assert!(apply(&handle, purge(set(), "a", &[1])).await.unwrap());
    assert_eq!(run(&handle, 2, 100).await, RebuildOutcome::Done);
    assert_eq!(generation(&handle, 2).await.max_epoch, OriginalEpoch(7));
}

fn accounts_page(rows: Vec<(Vec<u8>, Vec<u8>)>, more: bool) -> IoResult {
    IoResult::Page {
        rows: (rows.into_iter())
            .map(|(key, value)| (key.into(), value.into()))
            .collect(),
        next: more.then(|| b"next".to_vec().into()),
    }
}

fn answered(end: Result<StepEnd, RebuildError>) -> IoResult {
    IoResult::Child(ChildOutcome::new(end))
}

fn fresh() -> RangeAccount {
    RangeAccount::fresh(String::new(), None)
}

fn account_value(account: &RangeAccount) -> IoResult {
    IoResult::Values(vec![Some(account.to_bytes().unwrap().into())])
}

/// An active account without a rebuild begins one; malformed rows are skipped.
#[test]
fn begins_active_rebuilds() {
    let active = fresh().to_bytes().unwrap();
    let rows = vec![
        (b"short".to_vec(), active.clone()),
        (account_key(set(), range_id()), active),
    ];
    let mut begin = BeginRebuildsOperation::new(8, (2, 64));
    assert!(matches!(begin.first(), Ok(Next::Call(IoCall::Page { .. }))));
    let child = begin.next(accounts_page(rows, false));
    assert!(matches!(child, Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(begin.current, Some((set(), range_id())));
    let began: Result<RebuildOutcome, RebuildError> = Ok(RebuildOutcome::Paused);
    let began = IoResult::Child(ChildOutcome::new(began));
    assert_eq!(begin.next(began), Ok(Next::Done(1)));
}

/// A node without accounts begins nothing; a failed child is skipped; other results fail.
#[test]
fn begins_nothing_empty() {
    let mut begin = BeginRebuildsOperation::new(8, (2, 64));
    begin.first().unwrap();
    assert_eq!(begin.next(accounts_page(Vec::new(), false)), Ok(Next::Done(0)));

    let mut begin = BeginRebuildsOperation::new(8, (2, 64));
    begin.first().unwrap();
    let rows = vec![(account_key(set(), range_id()), fresh().to_bytes().unwrap())];
    begin.next(accounts_page(rows, false)).unwrap();
    let failed: Result<RebuildOutcome, RebuildError> = Err(IoError::Aborted.into());
    let failed = IoResult::Child(ChildOutcome::new(failed));
    assert_eq!(begin.next(failed), Ok(Next::Done(0)));

    let mut begin = BeginRebuildsOperation::new(8, (2, 64));
    begin.first().unwrap();
    assert!(begin.next(IoResult::Done).is_err());
}

// An active account starts a begin step; its commit continues from the new account.
#[test]
fn begin_step_advances() {
    let mut run = RebuildOperation::new(config(2, 0));
    let read = read_row(RANGE_ACCOUNT_KEYSPACE, account_key(set(), range_id()));
    assert_eq!(run.first(), Ok(Next::Call(read)));
    let step = run.next(account_value(&fresh()));
    assert!(matches!(step, Ok(Next::Call(IoCall::Child(_)))));
    let began = RangeAccount {
        rebuild: Some(Rebuild::new(2)),
        ..fresh()
    };
    let end = answered(Ok(StepEnd::Advanced {
        account: began,
        page: false,
    }));
    assert_eq!(run.next(end), Ok(Next::Done(RebuildOutcome::Paused)));
}

// A stale step and a lost commit read the account again until the budget runs out.
#[test]
fn conflicts_restart_then_pause() {
    let mut run = RebuildOperation::new(config(2, 1));
    run.first().unwrap();
    run.next(account_value(&fresh())).unwrap();
    let read = read_row(RANGE_ACCOUNT_KEYSPACE, account_key(set(), range_id()));
    assert_eq!(
        run.next(answered(Ok(StepEnd::Stale))),
        Ok(Next::Call(read.clone()))
    );
    run.next(account_value(&fresh())).unwrap();
    let lost = answered(Err(IoError::Conflict.into()));
    assert_eq!(run.next(lost), Ok(Next::Call(read)));
    run.conflicts = MAX_CONFLICTS;
    run.next(account_value(&fresh())).unwrap();
    assert_eq!(
        run.next(answered(Ok(StepEnd::Stale))),
        Ok(Next::Done(RebuildOutcome::Paused))
    );
}

// A page step re-reads the account, reads up to the probe, then writes its cursor and counts.
#[test]
fn page_step_counts() {
    let account = RangeAccount {
        rebuild: Some(Rebuild::new(2)),
        ..fresh()
    };
    let kind = StepKind::Page {
        walk: Walk::Uploads,
        probe_end: Some(b"row".to_vec()),
        terminal: false,
    };
    let mut step = RebuildStepOperation::new(config(2, 1), account.clone(), kind);
    assert_eq!(step.transaction(), TransactionMode::Write);
    assert!(matches!(step.first(), Ok(Next::Call(IoCall::Read(_)))));
    let Ok(Next::Call(IoCall::Effect(read))) = step.next(account_value(&account)) else {
        panic!("expected the page read");
    };
    let Effect::Storage(StorageEffect::IterRange { end, txn_id, .. }) = *read else {
        panic!("expected a range read");
    };
    assert_eq!(end, IterEnd::Through(b"row".to_vec().into()));
    assert_eq!(txn_id, None);
    let rows = IoResult::Event(Box::new(Event::Storage(StorageEvent::IterResult {
        values: vec![(b"row".to_vec().into(), Vec::new().into())],
        next_start_after: None,
    })));
    let Ok(Next::Call(IoCall::Write(writes))) = step.next(rows) else {
        panic!("expected the account row");
    };
    assert_eq!(writes.len(), 1);
    assert_eq!(
        step.next(IoResult::Done),
        Ok(Next::Call(IoCall::Delete(Vec::new())))
    );
    let Ok(Next::Call(IoCall::AddCounters { deltas, .. })) = step.next(IoResult::Done) else {
        panic!("expected the counted page");
    };
    assert_eq!(deltas[0].1.open_uploads, 1);
    let Ok(Next::Done(StepEnd::Advanced { account, page })) = step.next(IoResult::Done) else {
        panic!("expected the advanced account");
    };
    assert!(page);
    let cursor = account.rebuild.unwrap().uploads;
    assert_eq!(cursor, Cursor::At(b"row".to_vec()));
}

// An account another writer moved makes the step stale without any write.
#[test]
fn moved_account_stale() {
    let mut step = RebuildStepOperation::new(config(2, 1), fresh(), StepKind::Begin);
    step.first().unwrap();
    let moved = RangeAccount {
        rebuild: Some(Rebuild::new(2)),
        ..fresh()
    };
    assert_eq!(
        step.next(account_value(&moved)),
        Ok(Next::Done(StepEnd::Stale))
    );
}
