//! Tests the family prune pass by its calls and results: the guarded commit, stale rows, the
//! body pages behind a tombstone, empty families and the pass over the due rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::calls::ChildOutcome;
use aruna_core::keyspaces::{
    FAMILY_ALIAS_KEYSPACE, FAMILY_RECORD_KEYSPACE, FAMILY_TOMBSTONE_KEYSPACE,
};
use aruna_core::structs::execution::job::family::{
    ExpiryReason, JobRecordEnvelope, PhysicalExecutionState, SubmissionId,
};
use aruna_core::structs::execution::job::identity::{JobId, RETENTION_MS};
use aruna_core::structs::execution::job::prune::FamilyTombstone;
use aruna_core::structs::execution::job::record_keys::{alias_key, family_prefix, prune_rows_at};
use aruna_core::structs::execution::job::record_rows::to_bytes;
use aruna_core::types::Key;
use ulid::Ulid;

use super::commit::PruneCommitOperation;
use super::family::PruneFamilyOperation;
use super::pass::PruneFamiliesOperation;
use super::*;
use crate::jobs::lifecycle::LifecycleError;
use crate::jobs::lifecycle::cancel::PublishCancelOperation;
use crate::jobs::records::family::FamilyReadError;
use crate::jobs::route::JobRouteError;

fn family() -> JobFamilyId {
    JobFamilyId {
        submission_id: SubmissionId([5; 32]),
        request_digest: [6; 32],
    }
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn empty_page() -> IoResult {
    IoResult::Page {
        rows: Vec::new(),
        next: None,
    }
}

fn slot(due_ms: u64) -> Option<Value> {
    Some(Value::from(due_ms.to_be_bytes().as_slice()))
}

/// The call that commits `deletes` and `writes`, guarded by the family's due row 7 or not.
fn commit_call(
    guarded: bool,
    deletes: JobDeletes,
    writes: JobWrites,
) -> Result<Next<PruneStep>, PruneError> {
    let guard = guarded.then_some((family(), 7));
    let commit = PruneCommitOperation::new(guard, deletes, writes);
    Ok(Next::Call(IoCall::child(commit)))
}

// A commit refuses when the slot moved, and otherwise deletes, then writes.
#[test]
fn commit_checks_slot() {
    let rows = vec![(FAMILY_PRUNE_KEYSPACE.to_string(), prune_slot_key(&family()))];
    let mut moved = PruneCommitOperation::new(Some((family(), 7)), rows.clone(), Vec::new());
    assert!(matches!(moved.first(), Ok(Next::Call(IoCall::Read(_)))));
    assert_eq!(
        moved.next(IoResult::Values(vec![slot(8)])),
        Ok(Next::Done(false))
    );
    let writes = prune_rows_at(&family(), 9).to_vec();
    let mut kept = PruneCommitOperation::new(Some((family(), 7)), rows, writes);
    kept.first().unwrap();
    let deleting = kept.next(IoResult::Values(vec![slot(7)]));
    assert!(matches!(deleting, Ok(Next::Call(IoCall::Delete(_)))));
    assert!(matches!(
        kept.next(IoResult::Done),
        Ok(Next::Call(IoCall::Write(_)))
    ));
    assert_eq!(kept.next(IoResult::Done), Ok(Next::Done(true)));
}

// A due row the slot no longer names is dropped without a guard and nothing else is read.
#[test]
fn stale_row_dropped() {
    let mut prune = PruneFamilyOperation::new(family(), 7, None);
    assert!(matches!(prune.first(), Ok(Next::Call(IoCall::Read(_)))));
    let stale = vec![(
        FAMILY_PRUNE_KEYSPACE.to_string(),
        prune_due_key(7, &family()),
    )];
    let expected = commit_call(false, stale, Vec::new());
    assert_eq!(prune.next(IoResult::Values(vec![slot(9), None])), expected);
    let stored = child(Ok::<_, IoError>(true));
    assert_eq!(prune.next(stored), Ok(Next::Done(PruneStep::Moved)));
}

// Behind an unfinished tombstone the input pages go first, then the bodies page by page.
#[test]
fn tombstone_pages_bodies() {
    let tombstone = FamilyTombstone {
        pruned_at_ms: 1,
        last_record_ms: 1,
        finished: false,
        created_by: None,
        aliases: Vec::new(),
    };
    let stored = Some(Value::from(to_bytes(&tombstone).unwrap().as_slice()));
    let mut prune = PruneFamilyOperation::new(family(), 7, None);
    prune.first().unwrap();
    let pages = IoCall::child(super::pages::PrunePagesOperation::new(family()));
    let read = prune.next(IoResult::Values(vec![slot(7), stored]));
    assert_eq!(read, Ok(Next::Call(pages)));
    // Without own input pages left the body pages follow.
    let none = child(Ok::<JobDeletes, PruneError>(Vec::new()));
    assert!(matches!(
        prune.next(none),
        Ok(Next::Call(IoCall::Page { .. }))
    ));
    let key = Key::from(&b"record"[..]);
    let first = IoResult::Page {
        rows: vec![(key.clone(), Value::from(&[][..]))],
        next: None,
    };
    prune.next(first).unwrap();
    for _ in 2..BODY_SPACES.len() {
        prune.next(empty_page()).unwrap();
    }
    let body = vec![(FAMILY_RECORD_KEYSPACE.to_string(), key)];
    assert_eq!(
        prune.next(empty_page()),
        commit_call(true, body, Vec::new())
    );
    assert_eq!(
        prune.next(child(Ok::<_, IoError>(true))),
        Ok(Next::Done(PruneStep::Paged))
    );
}

// An empty family goes with its index rows; a fresh pending row moves the row instead.
#[test]
fn empty_family_goes() {
    let mut prune = PruneFamilyOperation::new(family(), 7, None);
    prune.first().unwrap();
    prune.next(IoResult::Values(vec![slot(7), None])).unwrap();
    let read = prune.next(child(Ok::<_, FamilyReadError>(
        Vec::<JobRecordEnvelope>::new(),
    )));
    assert!(matches!(read, Ok(Next::Call(IoCall::Page { .. }))));
    let gone = commit_call(true, index_rows(&family(), 7), Vec::new());
    assert_eq!(prune.next(empty_page()), gone);
    assert_eq!(
        prune.next(child(Ok::<_, IoError>(true))),
        Ok(Next::Done(PruneStep::Pruned))
    );
}

/// The decision on an empty family at `now_ms` behind the finished `tombstone`.
fn after_tombstone(
    tombstone: &FamilyTombstone,
    now_ms: u64,
) -> Result<Next<PruneStep>, PruneError> {
    let stored = Some(Value::from(to_bytes(tombstone).unwrap().as_slice()));
    let mut prune = PruneFamilyOperation::new(family(), 7, None);
    prune.stamp(now_ms);
    prune.first().unwrap();
    prune.next(IoResult::Values(vec![slot(7), stored])).unwrap();
    let records = Vec::<JobRecordEnvelope>::new();
    prune
        .next(child(Ok::<_, FamilyReadError>(records)))
        .unwrap();
    prune.next(empty_page())
}

// A finished tombstone waits for its expiry, then goes with its index rows and its aliases.
#[test]
fn tombstone_expires_late() {
    let job_id = JobId::from_bytes([4; 16]);
    let tombstone = FamilyTombstone {
        pruned_at_ms: 1_000,
        last_record_ms: 900,
        finished: true,
        created_by: None,
        aliases: vec![job_id],
    };
    let expiry = tombstone.expires_at_ms();
    let due = vec![(
        FAMILY_PRUNE_KEYSPACE.to_string(),
        prune_due_key(7, &family()),
    )];
    let waits = commit_call(true, due, prune_rows_at(&family(), expiry).to_vec());
    assert_eq!(after_tombstone(&tombstone, expiry - 1), waits);
    let mut gone = index_rows(&family(), 7);
    gone.push((
        FAMILY_ALIAS_KEYSPACE.to_string(),
        alias_key(job_id, &family()),
    ));
    gone.push((
        FAMILY_TOMBSTONE_KEYSPACE.to_string(),
        family_prefix(&family()),
    ));
    let expired = commit_call(true, gone, Vec::new());
    assert_eq!(after_tombstone(&tombstone, expiry), expired);
}

// An undecided family whose work all settled moves a retention later and is cancelled with an
// expiry once its row moved.
#[test]
fn undecided_family_expires() {
    let fixture = crate::tests::records::Family::new([48u8; 32]);
    let spec = fixture.spec();
    let records = fixture.run(1, 0, PhysicalExecutionState::Failed)[..2].to_vec();
    let now = spec.created_at_ms + 2 * RETENTION_MS;
    let mut prune = PruneFamilyOperation::new(fixture.family(), 7, None);
    prune.stamp(now);
    prune.first().unwrap();
    prune.next(IoResult::Values(vec![slot(7), None])).unwrap();
    prune
        .next(child(Ok::<_, FamilyReadError>(records)))
        .unwrap();
    let closures = prune.next(empty_page()).unwrap();
    assert!(matches!(closures, Next::Call(IoCall::Child(_))));
    let closed = child(Ok::<_, LifecycleError>(BTreeSet::<Ulid>::new()));
    prune.next(closed).unwrap();
    let due = vec![(
        FAMILY_PRUNE_KEYSPACE.to_string(),
        prune_due_key(7, &fixture.family()),
    )];
    let later = prune_rows_at(&fixture.family(), now + RETENTION_MS).to_vec();
    let moved = PruneCommitOperation::new(Some((fixture.family(), 7)), due, later);
    assert_eq!(
        prune.next(empty_page()),
        Ok(Next::Call(IoCall::child(moved)))
    );
    let cancel = PublishCancelOperation::expiry(spec, ExpiryReason::Retention, None, now);
    let asked = prune.next(child(Ok::<_, IoError>(true)));
    assert_eq!(asked, Ok(Next::Call(IoCall::child(cancel))));
    let published = child(Ok::<_, JobRouteError>(()));
    assert_eq!(prune.next(published), Ok(Next::Done(PruneStep::Moved)));
}

// The pass stops at the first row that is not due yet and reports when it is.
#[test]
fn stops_at_future() {
    let mut pass = PruneFamiliesOperation::new(None);
    pass.stamp(100);
    assert!(matches!(pass.first(), Ok(Next::Call(IoCall::Page { .. }))));
    let rows = vec![(prune_due_key(150, &family()), Value::from(&[][..]))];
    let page = IoResult::Page { rows, next: None };
    let Ok(Next::Done(outcome)) = pass.next(page) else {
        panic!("the pass ends at a future row");
    };
    assert_eq!(outcome.next_due_after, Some(Duration::from_millis(50)));
    assert_eq!(outcome.pruned, 0);
}
