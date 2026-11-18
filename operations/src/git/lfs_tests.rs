//! Tests LFS object checks call by call.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::super::testing::{auth, child, dataset, node};
use super::*;

fn stored(group_id: Option<Ulid>, holder: NodeId) -> StoredObject {
    StoredObject {
        node_id: holder,
        group_id,
        bucket: "bucket".to_string(),
        key: "key".to_string(),
        version_id: Ulid::from_bytes([3; 16]),
        size: 4,
        sha256: "ab".repeat(32),
        blake3: [1; 32],
        global: None,
    }
}

fn lfs(size: u64) -> LfsObject {
    LfsObject {
        oid: "ab".repeat(32),
        size,
    }
}

// An object with a malformed id is refused before anything is read.
#[test]
fn invalid_object_refused() {
    let object = LfsObject {
        oid: "x".to_string(),
        size: 1,
    };
    let mut inspect = InspectObjectOperation {
        object,
        open: None,
        locating: false,
    };
    assert_eq!(inspect.first(), Err(GitError::Invalid));
}

// Content no record names is looked up among this node's own copies.
#[test]
fn locate_falls_back() {
    let mut find = locate(&dataset(), &"cd".repeat(32));
    find.first().unwrap();
    let scanned = child(Ok::<_, GitError>(Vec::<GitRecord>::new()));
    assert!(matches!(find.next(scanned), Ok(Next::Call(_))));
    assert!(find.scanned);
    let found = Some(stored(None, node(2)));
    let answer = find.next(child(Ok::<_, GitError>(found.clone())));
    assert_eq!(answer, Ok(Next::Done(found)));
}

// A size other than the recorded one is invalid.
#[test]
fn size_must_match() {
    let mut inspect = InspectObjectOperation {
        object: lfs(9),
        open: None,
        locating: true,
    };
    let found = Some(stored(None, node(2)));
    let answer = inspect.next(child(Ok::<_, GitError>(found)));
    assert_eq!(answer, Err(GitError::Invalid));
}

// Without a known group only a read can be decided, by the source node.
#[test]
fn unknown_group_reads() {
    let object = stored(None, node(2));
    let mut write = PermitObjectOperation::new(&auth(), &object, Permission::WRITE);
    assert_eq!(write.first(), Err(GitError::Invalid));
    let mut read = PermitObjectOperation::new(&auth(), &object, Permission::READ);
    read.first().unwrap();
    let refused = BaoReadError::Refused(crate::replication::protocol::BaoReadRefusal::NotFound);
    let answer = read.next(child(Err::<BaoReadOutput, _>(refused)));
    assert_eq!(answer, Err(GitError::NotFound));
}

// A local object without a group cannot be checked and counts as unreadable.
#[test]
fn local_groupless_unreadable() {
    let mut read = ReadableOperation {
        node: Some(node(1)),
        auth: auth(),
        object: stored(None, node(1)),
    };
    assert_eq!(read.first(), Ok(Next::Done(false)));
}
