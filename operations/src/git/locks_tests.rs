//! Tests LFS lock claims call by call.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::git::GitRepository;

use super::super::testing::{auth, child, dataset, node, realm};
use super::*;

fn id() -> Ulid {
    dataset().document_id
}

fn opened() -> IoResult {
    let repository = GitRepository {
        document_id: id(),
        group_id: dataset().group_id,
        bucket: "bucket".to_string(),
    };
    child(Ok::<_, GitError>((dataset(), repository)))
}

fn record(lock: Ulid, path: &str, user: aruna_core::UserId) -> GitRecord {
    GitRecord {
        event_id: lock,
        realm_id: realm(),
        group_id: dataset().group_id,
        document_id: id(),
        placement: dataset().placement,
        user_id: user,
        node_id: node(1),
        occurred_at_ms: 1,
        change: GitChange::Lock {
            id: lock,
            path: path.to_string(),
        },
    }
}

fn other_user() -> aruna_core::UserId {
    aruna_core::UserId::new(Ulid::from_bytes([8; 16]), realm())
}

fn creating(path: &str) -> CreateLockOperation {
    CreateLockOperation {
        node: Some(node(1)),
        auth: auth(),
        id: id(),
        path: path.to_string(),
        open: Some(RepositoryOperation::new(
            Some(node(1)),
            &auth(),
            id(),
            Permission::WRITE,
        )),
        step: CreateStep::Open,
        document: None,
        claim: None,
    }
}

// A path outside the repository is refused before anything is read.
#[test]
fn invalid_path_refused() {
    assert_eq!(creating("../x").first(), Err(GitError::Invalid));
}

// A path another user holds is reported as taken, without a new claim.
#[test]
fn taken_path_reported() {
    let mut create = creating("a.bin");
    create.first().unwrap();
    create.next(opened()).unwrap();
    let held = vec![record(Ulid::from_bytes([5; 16]), "a.bin", other_user())];
    let answer = create.next(child(Ok::<_, GitError>(held)));
    assert!(matches!(answer, Ok(Next::Done(LockOutcome::Taken(_)))));
}

// A free path is claimed, and the claim decides once the records are read again.
#[test]
fn free_path_claimed() {
    let mut create = creating("a.bin");
    create.first().unwrap();
    create.next(opened()).unwrap();
    create
        .next(child(Ok::<_, GitError>(Vec::<GitRecord>::new())))
        .unwrap();
    assert_eq!(create.step, CreateStep::Publish);
    let claim = create.claim.unwrap();
    let published = record(claim, "a.bin", auth().user_id);
    create
        .next(child(Ok::<_, GitError>(published.clone())))
        .unwrap();
    let answer = create.next(child(Ok::<_, GitError>(vec![published])));
    assert!(matches!(answer, Ok(Next::Done(LockOutcome::Locked(_)))));
}

// Another user's lock stays unless the release is forced.
#[test]
fn unlock_needs_force() {
    let lock_id = Ulid::from_bytes([5; 16]);
    let mut unlock = UnlockOperation {
        node: Some(node(1)),
        auth: auth(),
        id: id(),
        lock_id,
        force: false,
        open: None,
        step: UnlockStep::Current,
        document: Some(dataset()),
        lock: None,
    };
    let held = vec![record(lock_id, "a.bin", other_user())];
    let answer = unlock.next(child(Ok::<_, GitError>(held)));
    assert_eq!(answer, Err(GitError::Locked("a.bin".to_string())));
}
