//! Tests the calls and results of the range purge and set clear steps.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::distribution::test_record::{binding, realm, record};
use aruna_core::calls::ChildOutcome;
use aruna_core::errors::StorageError;

fn target() -> PurgeTarget {
    let (snapshot, _) = record(true);
    PurgeTarget {
        realm_id: realm(),
        node_id: iroh::SecretKey::from_bytes(&[1; 32]).public(),
        bucket: "spire".to_string(),
        group_id: Ulid::from_bytes([2; 16]),
        binding: binding(&snapshot, 1, 5),
        range_id: Ulid::from_bytes([3; 16]),
        lower: String::new(),
        upper: None,
    }
}

fn no_row() -> IoResult {
    IoResult::Values(vec![None])
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn ok_step() -> IoResult {
    child::<(), PurgeError>(Ok(()))
}

fn bucket(distribution: Option<DistributionBinding>) -> IoResult {
    let info = BucketInfo {
        group_id: Ulid::from_bytes([2; 16]),
        created_at: std::time::SystemTime::UNIX_EPOCH,
        created_by: UserId::default(),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Default::default(),
        distribution,
    };
    child::<BucketInfo, GetBucketError>(Ok(info))
}

fn empty_range() -> IoResult {
    IoResult::Page {
        rows: Vec::new(),
        next: None,
    }
}

fn is_child<T>(next: Result<Next<T>, PurgeError>) -> bool {
    matches!(next, Ok(Next::Call(IoCall::Child(_))))
}

// A prefix is deleted page by page until a page comes back empty.
#[test]
fn clears_prefix_pages() {
    let mut clear = ClearPrefixOperation::new("rows", vec![1]);
    assert!(matches!(clear.first(), Ok(Next::Call(IoCall::Page { .. }))));
    let rows = vec![(Key::from(vec![1, 2]), Value::from(vec![9]))];
    let page = IoResult::Page { rows, next: None };
    let delete = vec![("rows".to_string(), Key::from(vec![1, 2]))];
    assert_eq!(clear.next(page), Ok(Next::Call(IoCall::Delete(delete))));
    assert!(matches!(
        clear.next(IoResult::Done),
        Ok(Next::Call(IoCall::Page { .. }))
    ));
    assert_eq!(clear.next(empty_range()), Ok(Next::Done(())));
}

// An absent attachment needs nothing; a cache entry that is gone leaves the totals alone.
#[test]
fn absent_copies_stop() {
    let mut drop = DropAttachmentOperation::new(Ulid::nil(), Ulid::nil());
    drop.first().unwrap();
    assert_eq!(drop.next(no_row()), Ok(Next::Done(())));
    let mut cached = DropCachedOperation::new([1; 32]);
    assert_eq!(cached.transaction(), TransactionMode::Write);
    cached.first().unwrap();
    let absent = IoResult::Values(vec![None, None]);
    assert_eq!(cached.next(absent), Ok(Next::Done(())));
}

// Without an account the page's rows go and no counter moves.
#[test]
fn debit_without_account() {
    let rows = vec![("rows".to_string(), Key::from(vec![1]))];
    let mut debit = DebitRowsOperation::new(&target(), rows.clone(), vec![vec![1]]);
    assert_eq!(debit.transaction(), TransactionMode::Write);
    assert!(matches!(debit.first(), Ok(Next::Call(IoCall::Read(_)))));
    let read = IoResult::Values(vec![None, None]);
    assert_eq!(debit.next(read), Ok(Next::Call(IoCall::Delete(rows))));
    assert_eq!(debit.next(IoResult::Done), Ok(Next::Done(())));
}

// A spent budget stops before any page; the cursor stays for the next pass.
#[test]
fn spent_budget_waits() {
    let mut purge = PurgeRangeOperation::new(target(), 0);
    purge.first().unwrap();
    assert_eq!(purge.next(no_row()), Ok(Next::Done((Purged::More, 0))));
}

// A bucket that is no longer the bound member ends the purge at once.
#[test]
fn unbound_bucket_ends() {
    let mut purge = PurgeRangeOperation::new(target(), 2);
    purge.first().unwrap();
    assert!(is_child(purge.next(no_row())));
    let Ok(Next::Call(IoCall::Write(_))) = purge.next(bucket(None)) else {
        panic!("expected the finished cursor");
    };
    assert_eq!(
        purge.next(IoResult::Done),
        Ok(Next::Done((Purged::Done, 1)))
    );
}

// Empty uploads move on to the inventory; an empty inventory clears the range and marks it.
#[test]
fn empty_range_clears() {
    let bound = target().binding;
    let mut purge = PurgeRangeOperation::new(target(), 4);
    purge.first().unwrap();
    purge.next(no_row()).unwrap();
    let page = purge.next(bucket(Some(bound.clone())));
    assert!(matches!(page, Ok(Next::Call(IoCall::Effect(_)))));
    assert!(matches!(
        purge.next(empty_range()),
        Ok(Next::Call(IoCall::Write(_)))
    ));
    assert!(purge.cursor.uploads_done);
    assert!(is_child(purge.next(IoResult::Done)));
    assert!(matches!(
        purge.next(bucket(Some(bound))),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    assert!(is_child(purge.next(empty_range())));
    assert!(is_child(purge.next(ok_step())));
    let marked = child::<(), MarkDirtyError>(Ok(()));
    assert!(matches!(
        purge.next(marked),
        Ok(Next::Call(IoCall::Write(_)))
    ));
    assert_eq!(
        purge.next(IoResult::Done),
        Ok(Next::Done((Purged::Done, 2)))
    );
}

// A failed bucket read fails the purge; a missing bucket does not.
#[test]
fn bucket_failure_fails() {
    let mut purge = PurgeRangeOperation::new(target(), 1);
    purge.first().unwrap();
    purge.next(no_row()).unwrap();
    let failed = child::<BucketInfo, GetBucketError>(Err(GetBucketError::StorageError(
        StorageError::Timeout,
    )));
    assert!(purge.next(failed).is_err());
}

fn clear_set(clearing: Option<DistributionBinding>) -> ClearSetOperation {
    let rows = (target().binding.set_id, "spire".to_string(), clearing);
    ClearSetOperation::new(rows, false, 8)
}

// A bucket bound by another member owns the rows: only the clear's cursor goes.
#[test]
fn replacement_ends_clear() {
    let mut clear = clear_set(None);
    clear.first().unwrap();
    assert!(is_child(clear.next(no_row())));
    let cursor = clear.cursor_row();
    assert_eq!(
        clear.next(bucket(Some(target().binding))),
        Ok(Next::Call(IoCall::Delete(vec![cursor])))
    );
    assert_eq!(
        clear.next(IoResult::Done),
        Ok(Next::Done((Purged::Done, 7)))
    );
}

// The clearing member's own binding does not stop the clear, and an empty page moves to the
// next keyspace.
#[test]
fn clear_walks_keyspaces() {
    let mut clear = clear_set(Some(target().binding));
    clear.first().unwrap();
    clear.next(no_row()).unwrap();
    let page = clear.next(bucket(Some(target().binding)));
    assert!(matches!(page, Ok(Next::Call(IoCall::Page { .. }))));
    assert_eq!(
        clear.next(empty_range()),
        Ok(Next::Call(IoCall::Delete(Vec::new())))
    );
    assert_eq!(clear.cursor.step, 1);
}

// A finished clear drops the fence row too unless it is kept.
#[test]
fn finished_clear_drops_fence() {
    let mut clear = clear_set(None);
    clear.cursor.step = u8::try_from(CLEARED.len() + 1).unwrap();
    assert!(matches!(clear.turn(), Ok(Next::Call(IoCall::Read(_)))));
    let Ok(Next::Call(IoCall::Delete(deletes))) = clear.next(no_row()) else {
        panic!("expected the last deletes");
    };
    assert_eq!(deletes.len(), 2);
    assert_eq!(
        clear.next(IoResult::Done),
        Ok(Next::Done((Purged::Done, 8)))
    );
}

// Looking at the uploads reads one page; a range without any has nothing to abort.
#[test]
fn looks_for_uploads() {
    let mut look = RangeUploadsOperation::new(target(), true);
    assert!(matches!(look.first(), Ok(Next::Call(IoCall::Effect(_)))));
    let found = look.next(empty_range());
    assert!(is_child(found));
    assert_eq!(look.next(ok_step()), Ok(Next::Done(false)));
}

// A transfer without kept imports has nothing to remove.
#[test]
fn absent_resume_stops() {
    let mut drop = DropResumeOperation::new(Ulid::nil());
    drop.first().unwrap();
    assert_eq!(drop.next(no_row()), Ok(Next::Done(())));
}
