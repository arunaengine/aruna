//! Tests the set counts that make bucket totals count a distributed bucket once.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::distribution::test_record::{record, store_record};
use aruna_core::structs::storage::distribution::snapshot::Deletion;

#[test]
fn counts_sets_once() {
    // Two member buckets of one set: a total of 5 node buckets holds 4 distinct buckets.
    let (snapshot, _) = record(true);
    let counts = SetCounts::of([&snapshot]);
    assert_eq!((counts.sets, counts.members), (1, 2));
    assert_eq!(counts.distinct(5), 4);

    // A failed member made no bucket, and a committed deletion purges the members' buckets.
    let mut failed = snapshot.clone();
    failed.config.members[1].state = MemberState::Failed;
    assert_eq!(SetCounts::of([&failed]).members, 1);
    let mut deleted = snapshot.clone();
    deleted.lifecycle.deletion = Some(Deletion {
        members: Vec::new(),
        fenced: Vec::new(),
        committed: true,
    });
    assert_eq!(SetCounts::of([&deleted]).distinct(5), 5);
}

#[tokio::test]
async fn counts_owner_group() {
    let (_temp, storage_handle) = crate::distribution::test_store::open();
    let context = DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };
    let (snapshot, bytes) = record(true);
    store_record(&context.storage_handle, snapshot.set_id(), bytes).await;
    let owner = snapshot.config.owner_group;

    let all = set_counts(&context, None).await.unwrap();
    assert_eq!((all.sets, all.members, all.complete), (1, 2, true));
    assert_eq!(set_counts(&context, Some(owner)).await.unwrap().sets, 1);
    assert_eq!(
        set_counts(&context, Some(ulid::Ulid::nil()))
            .await
            .unwrap()
            .sets,
        0
    );
}
