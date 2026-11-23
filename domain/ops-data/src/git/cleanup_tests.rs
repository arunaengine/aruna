//! Tests removing old Git objects call by call.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::super::testing::{asks_child, child, dataset, node, object, realm, record};
use super::*;

fn removing() -> RemoveObjectOperation {
    RemoveObjectOperation {
        node: Some(node(1)),
        realm_id: realm(),
        id: dataset().document_id,
        object: object(node(1)),
        key: vec![1; 48],
        step: RemoveStep::Document,
    }
}

fn pack_of(stored: &StoredObject) -> GitPack {
    GitPack {
        sha256: stored.sha256.clone(),
        size: stored.size,
    }
}

fn no_object() -> IoResult {
    child(Ok::<Option<StoredObject>, GitError>(None))
}

fn present() -> IoResult {
    child(Ok::<_, GitError>(Some(object(node(1)))))
}

fn history() -> IoResult {
    child(Ok::<_, GitError>(Vec::<GitRecord>::new()))
}

// A node without a network identity removes nothing.
#[test]
fn pass_needs_ids() {
    assert_eq!(remove_objects(None).first(), Ok(Next::Done(())));
}

// Only old packs and pack copies are touched.
#[test]
fn pass_skips_others() {
    let mut pass = remove_objects(Some((node(1), realm())));
    assert!(asks_child(&pass.first()));
    let mut other = object(node(1));
    other.key = "data/file".to_string();
    let rows = child(Ok::<ObjectRows, GitError>(vec![(vec![1; 48], other)]));
    assert_eq!(pass.next(rows), Ok(Next::Done(())));
}

#[test]
fn pass_visits_packs() {
    let mut pass = remove_objects(Some((node(1), realm())));
    pass.first().unwrap();
    let rows = child(Ok::<ObjectRows, GitError>(vec![(
        vec![1; 48],
        object(node(1)),
    )]));
    assert!(asks_child(&pass.next(rows)));
    assert_eq!(pass.next(child(Ok::<(), GitError>(()))), Ok(Next::Done(())));
}

// Of a deleted dataset the stored version goes first, then its index row.
#[test]
fn deleted_dataset_removed() {
    let mut remove = removing();
    assert!(asks_child(&remove.first()));
    let gone = child(Ok::<Option<Document>, StorageReadError>(None));
    assert!(asks_child(&remove.next(gone)));
    assert!(asks_child(&remove.next(present())));
    assert_eq!(remove.step, RemoveStep::Delete);
}

// A version that is already gone only loses its index row.
#[test]
fn gone_version_unindexed() {
    let mut remove = removing();
    remove.step = RemoveStep::Stored;
    assert!(asks_child(&remove.next(no_object())));
    assert_eq!(remove.step, RemoveStep::Index);
    assert_eq!(
        remove.next(child(Ok::<(), GitError>(()))),
        Ok(Next::Done(()))
    );
}

// An object that must stay is left alone.
#[test]
fn kept_object_stays() {
    let mut remove = removing();
    remove.step = RemoveStep::Settled;
    let keep = child(Ok::<Keep, GitError>(Some("needed")));
    assert_eq!(remove.next(keep), Ok(Next::Done(())));
}

fn judging(stored: &StoredObject) -> SettledOperation {
    let mut judge = settled(Some(node(1)), &dataset(), stored);
    assert!(asks_child(&judge.first()));
    judge
}

// An LFS copy no record names may go.
#[test]
fn unnamed_copy_goes() {
    let mut copy = object(node(1));
    copy.key = "git-copies/one".to_string();
    assert_eq!(judging(&copy).next(history()), Ok(Next::Done(None)));
}

// A pack is only judged after its bytes moved.
#[test]
fn pack_checks_move() {
    let mut judge = judging(&object(node(1)));
    assert!(asks_child(&judge.next(history())));
    assert_eq!(judge.step, SettleStep::Moved);
    assert_eq!(
        judge.next(child(Ok::<(), GitError>(()))),
        Ok(Next::Done(None))
    );
}

// A pack that could not move stays while its version still exists.
#[test]
fn unmoved_pack_stays() {
    let mut judge = judging(&object(node(1)));
    judge.step = SettleStep::Moved;
    let failed = child(Err::<(), _>(GitError::Unavailable));
    assert!(asks_child(&judge.next(failed)));
    assert_eq!(judge.next(present()), Err(GitError::Unavailable));
}

// Bytes found nowhere leave a row that points at nothing.
#[test]
fn lost_pack_dropped() {
    let mut judge = judging(&object(node(1)));
    judge.step = SettleStep::Moved;
    judge
        .next(child(Err::<(), _>(GitError::Unavailable)))
        .unwrap();
    assert_eq!(judge.next(no_object()), Ok(Next::Done(None)));
}

fn lfs_record(original: &StoredObject) -> GitRecord {
    record(
        1,
        GitChange::Objects {
            pack: None,
            refs: Vec::new(),
            lfs: vec![original.clone()],
            revision: None,
            digest: None,
            made: Vec::new(),
        },
    )
}

// The recorded original itself is never deleted as a copy.
#[test]
fn original_stays() {
    let mut copy = object(node(1));
    copy.key = "git-copies/one".to_string();
    let scanned = child(Ok::<_, GitError>(vec![lfs_record(&copy)]));
    let reason = "the copy is the recorded original";
    assert_eq!(judging(&copy).next(scanned), Ok(Next::Done(Some(reason))));
}

// A copy goes only while its original stays readable.
#[test]
fn copy_needs_original() {
    let mut copy = object(node(1));
    copy.key = "git-copies/one".to_string();
    let original = object(node(2));
    let scanned = || child(Ok::<_, GitError>(vec![lfs_record(&original)]));
    let mut judge = judging(&copy);
    assert!(asks_child(&judge.next(scanned())));
    assert_eq!(
        judge.next(child(Ok::<_, IoError>(true))),
        Ok(Next::Done(None))
    );
    let mut judge = judging(&copy);
    judge.next(scanned()).unwrap();
    let Ok(Next::Done(Some(_))) = judge.next(child(Ok::<_, IoError>(false))) else {
        panic!("expected a reason to keep the copy");
    };
}

fn moving() -> MovedOperation {
    let stored = object(node(1));
    let pack = pack_of(&stored);
    let digest = pack.digest().unwrap();
    MovedOperation {
        node: Some(node(1)),
        document: dataset(),
        pack,
        digest,
        history: Vec::new(),
        loaded: false,
    }
}

// Bytes already in Fjall need nothing more.
#[test]
fn moved_pack_done() {
    let mut moved = moving();
    assert!(asks_child(&moved.first()));
    let record = publish_record();
    assert_eq!(
        moved.next(child(Ok::<_, GitError>(Some(record)))),
        Ok(Next::Done(()))
    );
}

// A pack no record names has nothing left to move.
#[test]
fn unnamed_pack_done() {
    let mut moved = moving();
    moved.first().unwrap();
    let none = child(Ok::<Option<GitPackRecord>, GitError>(None));
    assert_eq!(moved.next(none), Ok(Next::Done(())));
}

// A named pack is moved by reading it once.
#[test]
fn named_pack_read() {
    let mut moved = moving();
    moved.history = vec![record(
        1,
        GitChange::Objects {
            pack: Some(moved.pack.clone()),
            refs: Vec::new(),
            lfs: Vec::new(),
            revision: None,
            digest: None,
            made: Vec::new(),
        },
    )];
    moved.first().unwrap();
    let none = child(Ok::<Option<GitPackRecord>, GitError>(None));
    assert!(asks_child(&moved.next(none)));
    let read = child(Ok::<_, GitError>(Bytes::from_static(b"pack")));
    assert_eq!(moved.next(read), Ok(Next::Done(())));
}

fn publish_record() -> GitPackRecord {
    let owner = record(1, GitChange::Unlock { id: Ulid::from(9) });
    super::super::publish::pack_record(&owner, Bytes::from_static(b"pack"))
}
