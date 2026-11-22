//! Tests choosing a dataset's storage location call by call.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::format::Compression;

use super::super::testing::{auth, child, dataset, node};
use super::*;

fn place(bucket: &str) -> DatasetLocation {
    DatasetLocation {
        bucket: bucket.to_string(),
        prefix: "data/".to_string(),
    }
}

fn bucket(group_id: GroupId) -> BucketInfo {
    BucketInfo {
        group_id,
        created_at: std::time::UNIX_EPOCH,
        created_by: auth().user_id,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: None,
    }
}

fn checking(kind: CheckKind) -> CheckLocationOperation {
    let group = dataset().group_id;
    CheckLocationOperation::new(Some(node(1)), &auth(), group, &place("other"), kind)
}

// The group's generated bucket needs no check, since it is created on first use.
#[test]
fn generated_bucket_passes() {
    let group = dataset().group_id;
    let location = place(&default_bucket(group));
    let kind = CheckKind::Default;
    let mut check = CheckLocationOperation::new(None, &auth(), group, &location, kind);
    assert_eq!(check.first(), Ok(Next::Done(())));
}

// A missing bucket stays not found for a dataset and is named for a group default.
#[test]
fn missing_bucket_differs() {
    let missing = || child(Err::<BucketInfo, _>(GetBucketError::NotFound));
    let mut chosen = checking(CheckKind::Chosen);
    chosen.first().unwrap();
    assert_eq!(chosen.next(missing()), Err(GitError::NotFound));
    let mut default = checking(CheckKind::Default);
    default.first().unwrap();
    assert!(matches!(default.next(missing()), Err(GitError::Refused(_))));
}

// A group default must use a bucket of that group.
#[test]
fn default_needs_group() {
    let mut check = checking(CheckKind::Default);
    check.first().unwrap();
    let foreign = Ok::<_, GetBucketError>(bucket(Ulid::from_bytes([9; 16])));
    assert!(matches!(
        check.next(child(foreign)),
        Err(GitError::Refused(_))
    ));
}

// A group without a stored choice uses the generated default.
#[test]
fn group_default_generated() {
    let group = dataset().group_id;
    let mut read = group_default(group);
    read.first().unwrap();
    let answer = read.next(IoResult::Values(vec![None]));
    assert_eq!(
        answer,
        Ok(Next::Done(DatasetLocation::group_default(group)))
    );
}

// The stored location of a reduced state wins over the default.
#[test]
fn chosen_prefers_record() {
    let (location, default) = chosen(&dataset(), Some((Ulid::nil(), place("other"))));
    assert_eq!((location, default), (place("other"), false));
    assert!(chosen(&dataset(), None).1);
}
