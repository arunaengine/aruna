//! Tests generating snapshots call by call.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::git::GitChange;

use super::super::testing::{
    asks_child, asks_store, auth, child, dataset, finished, node, projection, ran, realm, record,
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
    // The link search was timed, so its report follows the outcome.
    assert_eq!(finished(run.next(child(Ok::<(), GitError>(())))), ((), 1));
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

// Each stage is timed from its call to its answer on the runner's clock.
#[test]
fn stages_use_stamps() {
    let source = (event_id(), "{}".to_string());
    let ids = Some((node(1), realm()));
    let mut run = generate(ids, &dataset(), &projection(&[]), source, None);
    run.first().unwrap();
    let event = child(Ok::<Option<MetadataEventRecord>, GitError>(None));
    run.next(event).unwrap();
    run.stamp(10);
    let none = child(Ok::<Option<String>, GitError>(None));
    assert!(asks_child(&run.next(none)));
    run.stamp(25);
    let written = child(Ok::<_, GitError>("Edit".to_string()));
    assert!(asks_child(&run.next(written)));
    let reports = run.abort();
    assert_eq!(reports.len(), 1);
    let IoCall::Effect(effect) = &reports[0] else {
        panic!("expected the stage report");
    };
    let aruna_core::effects::Effect::Git(call) = &**effect else {
        panic!("expected a Git call");
    };
    let expected = aruna_core::git::GitCall::Stages(vec![("git_subject", 15), ("git_linked", 0)]);
    assert_eq!(**call, expected);
}
