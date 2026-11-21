//! Tests reading Git packs and LFS content call by call.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;

use aruna_core::events::{GroupEvent, GroupResult};
use aruna_core::git::{GitAnswer, GitChange, GitRepository};

use super::super::testing::{
    answered, asks_child, auth, child, dataset, node, object, realm, record,
};
use super::*;

fn pack() -> GitPack {
    GitPack {
        sha256: "ab".repeat(32),
        size: 4,
    }
}

fn owner() -> GitRecord {
    record(1, GitChange::Unlock { id: Ulid::from(9) })
}

fn reading() -> PackBytesOperation {
    let mut read = pack_bytes(Some(node(1)), &dataset(), &pack(), &owner());
    assert!(asks_child(&read.first()));
    read
}

fn no_stream() -> IoResult {
    child(Ok::<_, BaoReadError>(BaoReadOutput::Metadata {
        size: 4,
        blake3: [1; 32],
        etag: None,
        hashes: HashMap::new(),
    }))
}

// A pack already kept in Fjall needs no object read.
#[test]
fn stored_pack_returned() {
    let bytes = Bytes::from_static(b"pack");
    let stored = publish::pack_record(&owner(), bytes.clone());
    let answer = reading().next(child(Ok::<_, GitError>(Some(stored))));
    assert_eq!(answer, Ok(Next::Done(bytes)));
}

// A pack neither in Fjall nor kept as an object cannot be read here.
#[test]
fn missing_copy_unavailable() {
    let mut read = reading();
    let none = || child(Ok::<Option<GitPackRecord>, GitError>(None));
    assert!(asks_child(&read.next(none())));
    let copy = child(Ok::<Option<StoredObject>, GitError>(None));
    assert_eq!(read.next(copy), Err(GitError::Unavailable));
}

// Bytes that do not match the recorded pack are refused.
#[test]
fn wrong_bytes_refused() {
    let mut read = reading();
    read.step = PackStep::Collect;
    let bytes = answered(GitAnswer::Collected(Bytes::from_static(b"other")));
    assert_eq!(read.next(bytes), Err(GitError::Invalid));
}

#[test]
fn open_needs_node() {
    let mut read = open(None, &auth(), &dataset(), &object(node(2)), &[]);
    assert_eq!(read.first(), Err(GitError::Unavailable));
}

// The recording node comes first, then every other holder; none left means unavailable.
#[test]
fn open_tries_holders() {
    let holders = [node(1), node(2), node(3)];
    let mut read = open(
        Some(node(1)),
        &auth(),
        &dataset(),
        &object(node(2)),
        &holders,
    );
    assert!(asks_child(&read.first()));
    let none = child(Ok::<Option<StoredObject>, GitError>(None));
    assert!(asks_child(&read.next(none)));
    assert_eq!(read.sources.len(), 1);
    assert!(asks_child(&read.next(no_stream())));
    assert_eq!(read.next(no_stream()), Err(GitError::Unavailable));
}

// Content no record or upload names is not found.
#[test]
fn unknown_content_missing() {
    let mut read = download(
        Some((node(1), realm())),
        &auth(),
        dataset().document_id,
        "oid",
    );
    assert!(asks_child(&read.first()));
    let repository = GitRepository {
        document_id: dataset().document_id,
        group_id: dataset().group_id,
        bucket: "bucket".to_string(),
    };
    let opened = child(Ok::<Repository, GitError>((dataset(), repository)));
    assert!(asks_child(&read.next(opened)));
    let located = child(Ok::<Option<StoredObject>, GitError>(None));
    assert_eq!(read.next(located), Err(GitError::NotFound));
}

// A version the resolver did not find in time has no holders.
#[test]
fn slow_resolver_empty() {
    let results = vec![GroupResult::Unfinished];
    let answer = version_answer(IoResult::Group(GroupEvent::Finished { results }));
    assert_eq!(answer, Ok(None));
}
