//! Tests repository access call by call.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::super::testing::{auth, child, dataset, node};
use super::*;

fn checking() -> DocumentAccessOperation {
    document(&auth(), dataset().document_id, Permission::READ)
}

// An unknown document is not found before any permission is checked.
#[test]
fn unknown_document_missing() {
    let mut access = checking();
    access.first().unwrap();
    let answer = access.next(child(Ok::<Option<Document>, StorageReadError>(None)));
    assert_eq!(answer, Err(GitError::NotFound));
}

// A refused permission ends the check with that refusal.
#[test]
fn refusal_stops_access() {
    let mut access = checking();
    access.first().unwrap();
    let found = Ok::<_, StorageReadError>(Some(dataset()));
    assert!(matches!(access.next(child(found)), Ok(Next::Call(_))));
    let refused = child(Err::<(), _>(AuthorizeError::PermissionDenied));
    let answer = access.next(refused);
    let expected = GitError::Authorization(AuthorizeError::PermissionDenied);
    assert_eq!(answer, Err(expected));
}

// The repository names the group's dataset bucket once this node holds the document.
#[test]
fn repository_names_bucket() {
    let id = dataset().document_id;
    let mut open = RepositoryOperation::new(Some(node(1)), &auth(), id, Permission::READ);
    open.first().unwrap();
    open.next(child(Ok::<_, GitError>(dataset()))).unwrap();
    let held = child(Ok::<_, GitError>(vec![node(1)]));
    let Ok(Next::Done((_, repository))) = open.next(held) else {
        panic!("the repository is ready");
    };
    assert_eq!(repository.bucket, default_bucket(dataset().group_id));
}
