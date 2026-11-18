//! Tests serving Git over HTTP call by call.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::Effect;
use aruna_core::git::{GitCall, GitRepository};
use bytes::Bytes;

use super::super::testing::{
    asks_child, asks_store, auth, child, dataset_of, git_node, projection, ran,
};
use super::*;

fn repo(document: &MetadataRegistryRecord) -> GitRepository {
    GitRepository {
        document_id: document.document_id,
        group_id: document.group_id,
        bucket: "bucket".to_string(),
    }
}

fn request(document: &MetadataRegistryRecord, action: &str) -> GitRequest {
    GitRequest {
        repository: repo(document),
        method: "POST".to_string(),
        action: action.to_string(),
        query: String::new(),
        content_type: String::new(),
        content_encoding: String::new(),
        protocol: String::new(),
        body: Bytes::new(),
        token: String::new(),
        lfs_url: String::new(),
        metadata_url: String::new(),
        push_key: String::new(),
    }
}

fn opened(document: &MetadataRegistryRecord) -> IoResult {
    child(Ok::<Repository, GitError>((
        document.clone(),
        repo(document),
    )))
}

fn served() -> GitEvent {
    GitEvent::Response {
        status: 200,
        headers: Default::default(),
        body: Bytes::new(),
    }
}

// Reads need no hold; a push keeps the repository for its whole run.
#[test]
fn only_pushes_hold() {
    let document = dataset_of(60);
    let read = transport(&git_node(), &auth(), request(&document, "git-upload-pack"));
    assert_eq!(read.hold_scope(), None);
    let push = transport(&git_node(), &auth(), request(&document, "git-receive-pack"));
    let scope = HoldScope::GitRepository(document.document_id);
    assert_eq!(push.hold_scope(), Some(scope));
}

// A request for another repository than the document's is refused.
#[test]
fn other_repository_refused() {
    let document = dataset_of(61);
    let mut asked = request(&document, "git-upload-pack");
    asked.repository.bucket = "other".to_string();
    let mut serve = transport(&git_node(), &auth(), asked);
    assert!(asks_child(&serve.first()));
    assert_eq!(serve.next(opened(&document)), Err(GitError::Conflict));
}

// A read is served from the viewed cache and applies nothing.
#[test]
fn read_served_directly() {
    let document = dataset_of(62);
    let asked = request(&document, "git-upload-pack");
    let mut serve = transport(&git_node(), &auth(), asked);
    serve.first().unwrap();
    assert!(asks_child(&serve.next(opened(&document))));
    let viewed = child(Ok::<_, GitError>((projection(&[]), false)));
    assert!(asks_store(&serve.next(viewed)));
    assert_eq!(serve.next(ran(served())), Ok(Next::Done(served())));
}

// A push runs under a key and applies pushed metadata; a failure there does not fail it.
#[test]
fn push_applies_metadata() {
    let document = dataset_of(63);
    let asked = request(&document, "git-receive-pack");
    let mut serve = transport(&git_node(), &auth(), asked);
    serve.first().unwrap();
    assert!(asks_child(&serve.next(opened(&document))));
    let refreshed = child(Ok::<_, GitError>(projection(&[])));
    let Ok(Next::Call(IoCall::Effect(effect))) = serve.next(refreshed) else {
        panic!("expected the store call");
    };
    let Effect::Git(call) = *effect else {
        panic!("expected a Git call");
    };
    assert!(matches!(*call, GitCall::Push { .. }));
    assert!(asks_child(&serve.next(ran(served()))));
    let waits = child(Err::<(), _>(GitError::Unavailable));
    assert_eq!(serve.next(waits), Ok(Next::Done(served())));
}
