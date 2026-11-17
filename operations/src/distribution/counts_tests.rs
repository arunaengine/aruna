//! Tests the set counts that make bucket totals count a distributed bucket once.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::distribution::test_record::{record, store_record};
use crate::driver::{DriverContext, drive};
use aruna_core::calls::IoCall;
use aruna_core::types::{Key, Value};
use aruna_core::structs::storage::distribution::snapshot::Deletion;

#[test]
fn counts_sets_once() {
    // Two member buckets of one set: a total of 5 node buckets holds 4 distinct buckets.
    let (snapshot, _) = record(true);
    let counts = SetCounts::of([&snapshot]);
    assert_eq!((counts.sets, counts.members), (1, 2));
    assert_eq!(counts.distinct(5), Some(4));

    // A failed member made no bucket, and a committed deletion purges the members' buckets.
    let mut failed = snapshot.clone();
    failed.config.members[1].state = MemberState::Failed;
    assert_eq!(SetCounts::of([&failed]).members, 1);
}

#[test]
fn deleting_set_unknown() {
    // A committed deletion: its members purge at their own pace, so no total is passed off as
    // exact, neither with nor without their buckets. Another live set does not change that.
    let (snapshot, _) = record(true);
    let mut deleted = snapshot.clone();
    deleted.lifecycle.deletion = Some(Deletion {
        members: Vec::new(),
        fenced: Vec::new(),
        committed: true,
    });
    let counts = SetCounts::of([&deleted, &snapshot]);
    assert_eq!((counts.sets, counts.deleting), (1, 1));
    assert_eq!(counts.distinct(7), None);

    // A deletion only prepared still has every member bucket: the set counts once as before.
    let mut prepared = deleted.clone();
    prepared.lifecycle.deletion.as_mut().unwrap().committed = false;
    assert_eq!(SetCounts::of([&prepared]).distinct(5), Some(4));
}

#[tokio::test]
async fn counts_owner_group() {
    let (_temp, storage_handle) = crate::distribution::test_store::open();
    let context = DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };
    let (snapshot, bytes) = record(true);
    store_record(&context.storage_handle, snapshot.set_id(), bytes).await;
    let owner = snapshot.config.owner_group;

    let all = drive(SetCountsOperation::new(None), &context)
        .await
        .unwrap();
    assert_eq!((all.sets, all.members, all.complete), (1, 2, true));
    assert_eq!(
        drive(SetCountsOperation::new(Some(owner)), &context)
            .await
            .unwrap()
            .sets,
        1
    );
    assert_eq!(
        drive(SetCountsOperation::new(Some(ulid::Ulid::nil())), &context)
            .await
            .unwrap()
            .sets,
        0
    );
}

#[tokio::test]
async fn reads_deleting_record() {
    use crate::distribution::test_record::{realm, secret};
    use aruna_core::structs::storage::distribution::{DistributionView, SignedSnapshot};
    let (_temp, storage_handle) = crate::distribution::test_store::open();
    let context = DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };
    let (mut snapshot, _) = record(true);
    snapshot.lifecycle.deletion = Some(Deletion {
        members: Vec::new(),
        fenced: Vec::new(),
        committed: true,
    });
    let view = DistributionView {
        head: SignedSnapshot::sign(snapshot.clone(), realm(), &secret(1)).unwrap(),
        conflict: None,
    };
    store_record(
        &context.storage_handle,
        snapshot.set_id(),
        view.to_bytes().unwrap(),
    )
    .await;

    let counts = drive(
        SetCountsOperation::new(Some(snapshot.config.owner_group)),
        &context,
    )
    .await
    .unwrap();
    assert_eq!((counts.deleting, counts.distinct(5)), (1, None));
}

fn page(after: Option<Key>) -> Next<SetCounts> {
    Next::Call(IoCall::Page {
        space: DISTRIBUTION_KEYSPACE.to_string(),
        prefix: Key::from(Vec::new()),
        after,
        limit: PAGE,
    })
}

fn rows(rows: Vec<(Key, Value)>, next: Option<Key>) -> IoResult {
    IoResult::Page { rows, next }
}

fn full() -> Vec<(Key, Value)> {
    vec![(Key::from(vec![1]), Value::from(vec![0])); PAGE]
}

// Pages are read until the records end; undecodable records and other groups' sets are left out.
#[test]
fn pages_until_end() {
    let (snapshot, bytes) = record(true);
    let mut operation = SetCountsOperation::new(Some(snapshot.config.owner_group));
    assert_eq!(operation.first(), Ok(page(None)));
    let next = Key::from(vec![2]);
    let first = operation.next(rows(full(), Some(next.clone())));
    assert_eq!(first, Ok(page(Some(next))));
    let last = rows(vec![(Key::from(vec![3]), bytes.into())], None);
    let Ok(Next::Done(counts)) = operation.next(last) else {
        panic!("expected the counts");
    };
    assert_eq!((counts.sets, counts.members, counts.complete), (1, 2, true));
}

// Records beyond the read limit leave the counts incomplete.
#[test]
fn limit_marks_incomplete() {
    let mut operation = SetCountsOperation::new(None);
    operation.first().unwrap();
    let mut last = None;
    for _ in 0..MAX_SETS / PAGE {
        last = Some(operation.next(rows(full(), Some(Key::from(vec![2])))));
    }
    let Some(Ok(Next::Done(counts))) = last else {
        panic!("expected the counts");
    };
    assert!(!counts.complete);
}

// A result other than a page ends the count with an error.
#[test]
fn rejects_unexpected() {
    let mut operation = SetCountsOperation::new(None);
    operation.first().unwrap();
    assert!(matches!(
        operation.next(IoResult::Done),
        Err(CountsError::Io(IoError::Unexpected { .. }))
    ));
}
