//! Tests storing Git content call by call.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::super::testing::{auth, child, dataset, node};
use super::*;

fn stored() -> StoredObject {
    StoredObject {
        node_id: node(1),
        group_id: Some(dataset().group_id),
        bucket: "bucket".to_string(),
        key: "key".to_string(),
        version_id: Ulid::from_bytes([3; 16]),
        size: 4,
        sha256: "ab".repeat(32),
        blake3: [1; 32],
        global: None,
    }
}

fn bucket(group_id: GroupId) -> BucketInfo {
    BucketInfo {
        group_id,
        created_at: UNIX_EPOCH,
        created_by: auth().user_id,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: None,
    }
}

fn describing(version: Option<Ulid>) -> DescribeObjectOperation {
    DescribeObjectOperation {
        node: Some(node(1)),
        name: "bucket".to_string(),
        group_id: dataset().group_id,
        key: "key".to_string(),
        version,
    }
}

// A dataset bucket of another group is a conflict, never adopted.
#[test]
fn foreign_bucket_conflicts() {
    let mut ensure = EnsureBucketOperation::new(&dataset(), &auth());
    ensure.first().unwrap();
    let foreign = bucket(Ulid::from_bytes([9; 16]));
    let answer = ensure.next(child(Ok::<_, GetBucketError>(foreign)));
    assert_eq!(answer, Err(GitError::Conflict));
}

// A missing bucket is created, and losing that race reads the winner.
#[test]
fn missing_bucket_created() {
    let mut ensure = EnsureBucketOperation::new(&dataset(), &auth());
    ensure.first().unwrap();
    ensure
        .next(child(Err::<BucketInfo, _>(GetBucketError::NotFound)))
        .unwrap();
    assert_eq!(ensure.step, BucketStep::Created);
    let raced = Err::<BucketInfo, _>(CreateBucketError::BucketAlreadyExists);
    ensure.next(child(raced)).unwrap();
    assert_eq!(ensure.step, BucketStep::Reread);
    let found = bucket(dataset().group_id);
    let answer = ensure.next(child(Ok::<_, GetBucketError>(found.clone())));
    assert_eq!(answer, Ok(Next::Done(found)));
}

// A missing current object is no object; a missing pinned version is a failure.
#[test]
fn missing_version_fails() {
    let missing = || child(Err::<HeadObjectResult, _>(HeadObjectError::NoSuchKey));
    let mut current = describing(None);
    current.first().unwrap();
    assert_eq!(current.next(missing()), Ok(Next::Done(None)));
    let mut pinned = describing(Some(Ulid::from_bytes([3; 16])));
    pinned.first().unwrap();
    assert_eq!(pinned.next(missing()), Err(GitError::Unavailable));
}

// A pack over the limit is refused with the advice to use LFS.
#[test]
fn oversized_pack_refused() {
    let pack = Bytes::from(vec![0; MAX_PACK_BYTES + 1]);
    assert!(matches!(describe_pack(&pack), Err(GitError::Refused(_))));
}

// Content this node already stored is answered without a write.
#[test]
fn known_content_reused() {
    let body: Blob = BackendStream(Box::pin(futures_util::stream::empty()));
    let mut store = StoreObjectOperation {
        node: Some(node(1)),
        routing: Default::default(),
        auth: auth(),
        document: dataset(),
        key: "key".to_string(),
        size: 4,
        sha256: stored().sha256,
        body: Some(body),
        step: StoreStep::Copy,
    };
    store.first().unwrap();
    let answer = store.next(child(Ok::<_, GitError>(Some(stored()))));
    assert_eq!(answer, Ok(Next::Done(stored())));
}
