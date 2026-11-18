//! Tests generating snapshots call by call.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::git::GitChange;

use super::super::testing::{
    asks_child, asks_store, auth, child, dataset, node, projection, ran, realm, record,
};
use super::*;

fn event_id() -> Ulid {
    Ulid::from(5)
}

/// A generation stepped to the store call that makes the snapshot.
fn generating(refs: &[(&str, &str)]) -> GenerateOperation {
    let source = (event_id(), "{}".to_string());
    let ids = Some((node(1), realm()));
    let mut run = generate(ids, &dataset(), &projection(refs), source, Some([1; 32]));
    assert!(asks_child(&run.first()));
    let event = child(Ok::<Option<MetadataEventRecord>, GitError>(None));
    assert!(asks_child(&run.next(event)));
    let message = child(Ok::<_, GitError>(Some("Edit".to_string())));
    assert!(asks_child(&run.next(message)));
    let links = child(Ok::<_, GitError>(Vec::<LinkedObject>::new()));
    assert!(asks_store(&run.next(links)));
    run
}

fn generated(main: Option<&str>) -> IoResult {
    ran(GitEvent::Generated {
        aruna: "a".to_string(),
        main: main.map(str::to_owned),
    })
}

fn published() -> IoResult {
    let change = GitChange::Unlock { id: Ulid::from(9) };
    child(Ok::<_, GitError>(record(1, change)))
}

// A document without a snapshot branch has nothing to compare against.
#[test]
fn first_subject_direct() {
    let mut read = subject(&dataset(), &BTreeMap::new(), "{}", auth().user_id);
    assert!(matches!(read.first(), Ok(Next::Done(_))));
}

// The ARC graph file comes first, then the RO-Crate, then no previous metadata.
#[test]
fn subject_tries_files() {
    let refs = BTreeMap::from([("refs/heads/aruna".to_string(), "a".to_string())]);
    let mut read = subject(&dataset(), &refs, "{}", auth().user_id);
    assert!(asks_store(&read.first()));
    assert!(asks_store(&read.next(ran(GitEvent::File(None)))));
    assert!(matches!(
        read.next(ran(GitEvent::File(None))),
        Ok(Next::Done(_))
    ));
}

// A conversion failure becomes the document's status and is no error.
#[test]
fn failure_stored_status() {
    let mut run = generating(&[]);
    let failed = ran(GitEvent::GenerateFailed("no ISA".to_string()));
    assert!(asks_child(&run.next(failed)));
    assert_eq!(run.step, GenerateStep::Status);
    assert_eq!(run.next(child(Ok::<(), GitError>(()))), Ok(Next::Done(())));
}

// New commits are packed, published and noted as imported before the status is stored.
#[test]
fn snapshot_published() {
    let mut run = generating(&[]);
    assert!(asks_store(&run.next(generated(Some("m")))));
    let pack = ran(GitEvent::Packed(Bytes::from_static(b"pack")));
    assert!(asks_child(&run.next(pack)));
    assert!(asks_child(&run.next(published())));
    assert_eq!(run.step, GenerateStep::Mark);
    assert!(asks_child(
        &run.next(child(Err::<(), _>(IoError::Unfinished)))
    ));
    assert_eq!(run.step, GenerateStep::Status);
}

// An unchanged snapshot is recorded without a pack.
#[test]
fn unchanged_skips_pack() {
    let mut run = generating(&[("refs/heads/aruna", "a")]);
    assert!(asks_child(&run.next(generated(None))));
    assert_eq!(run.step, GenerateStep::Publish);
    assert!(asks_child(&run.next(published())));
    assert_eq!(run.step, GenerateStep::Status);
}

// A checkpoint is an ordinary record of its own.
#[test]
fn checkpoint_publishes() {
    let ids = Some((node(1), realm()));
    let mut fold = checkpoint(ids, &dataset(), &projection(&[]));
    assert!(fold.first().is_ok());
}
