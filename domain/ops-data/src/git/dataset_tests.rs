//! Tests storing pushed data files call by call.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::SystemTime;

use aruna_core::git::FileChange;
use aruna_core::structs::storage::format::Compression;

use super::super::testing::{
    asks_child, asks_store, auth, child, dataset, git_node, node, object, ran,
};
use super::*;

fn merge() -> PendingMerge {
    PendingMerge {
        user_id: auth().user_id,
        old: ZERO_OID.into(),
        new: "a".repeat(40),
    }
}

fn location() -> DatasetLocation {
    let document = dataset();
    DatasetLocation::default_for(document.group_id, document.document_id)
}

fn bucket() -> BucketInfo {
    BucketInfo {
        group_id: dataset().group_id,
        created_at: SystemTime::UNIX_EPOCH,
        created_by: auth().user_id,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: None,
    }
}

fn push_target(bucket: Option<BucketInfo>) -> PushTarget {
    PushTarget {
        location: location(),
        bucket,
        changed: BTreeSet::from(["data/a.csv".to_string()]),
        stored: BTreeMap::new(),
    }
}

fn filing() -> StoreFileOperation {
    let scope = (&dataset(), &merge(), &location());
    let mut store = store_file(&git_node(), scope, bucket(), "data/a.csv");
    assert!(asks_store(&store.first()));
    store
}

fn read_file() -> IoResult {
    ran(GitEvent::File(Some(Bytes::from_static(b"text"))))
}

fn allowed() -> IoResult {
    child(Ok::<(), AuthorizeError>(()))
}

// An ARC push stores no files.
#[test]
fn arc_push_skipped() {
    let mut find = target(&git_node(), &dataset(), &merge());
    assert!(asks_store(&find.first()));
    let arc = ran(GitEvent::Layout(Some(Layout::Arc)));
    assert_eq!(find.next(arc), Ok(Next::Done(None)));
}

// A location that is not writable here leaves the files for a later push.
#[test]
fn unwritable_location_tolerated() {
    let mut find = target(&git_node(), &dataset(), &merge());
    find.first().unwrap();
    assert!(asks_child(
        &find.next(ran(GitEvent::Layout(Some(Layout::RoCrate))))
    ));
    let chosen = child(Ok::<_, GitError>((location(), true)));
    assert!(asks_child(&find.next(chosen)));
    let refused = child(Err::<BucketInfo, _>(GitError::NotFound));
    assert!(asks_store(&find.next(refused)));
    let changes = vec![
        FileChange {
            path: "data/a.csv".to_string(),
            change: FileChangeKind::Added,
        },
        FileChange {
            path: "data/gone.csv".to_string(),
            change: FileChangeKind::Deleted,
        },
    ];
    let Ok(Next::Done(Some(found))) = find.next(ran(GitEvent::Diff(changes))) else {
        panic!("expected the target");
    };
    assert_eq!(found.bucket, None);
    assert_eq!(found.changed, BTreeSet::from(["data/a.csv".to_string()]));
}

// Without a bucket nothing is stored and the graph stays as it is.
#[test]
fn bucketless_skips_files() {
    let target = push_target(None);
    let mut store = store_files(&git_node(), &dataset(), &merge(), target, "{}");
    assert_eq!(store.first(), Ok(Next::Done((push_target(None), None))));
}

#[test]
fn invalid_graph_refused() {
    let target = push_target(Some(bucket()));
    let mut store = store_files(&git_node(), &dataset(), &merge(), target, "not json");
    assert_eq!(store.first(), Err(GitError::Invalid));
}

// A file the commit does not have is skipped.
#[test]
fn missing_file_skipped() {
    let answer = filing().next(ran(GitEvent::File(None)));
    assert_eq!(answer, Ok(Next::Done(None)));
}

// Writing needs the caller's permission on the key.
#[test]
fn file_write_authorized() {
    let mut store = filing();
    assert!(asks_child(&store.next(read_file())));
    let refused = child(Err::<(), _>(AuthorizeError::PermissionDenied));
    let expected = GitError::Authorization(AuthorizeError::PermissionDenied);
    assert_eq!(store.next(refused), Err(expected));
}

// Content already stored at the key is reused.
#[test]
fn same_content_reused() {
    let mut store = filing();
    store.next(read_file()).unwrap();
    assert!(asks_child(&store.next(allowed())));
    let mut existing = object(node(1));
    existing.sha256 = Content::Blob(Bytes::from_static(b"text")).sha256();
    let answer = store.next(child(Ok::<_, GitError>(Some(existing.clone()))));
    let Ok(Next::Done(Some(file))) = answer else {
        panic!("expected the stored file");
    };
    assert_eq!(file.hash, existing.blake3);
}

// Other content at the key is written.
#[test]
fn new_content_written() {
    let mut store = filing();
    store.next(read_file()).unwrap();
    store.next(allowed()).unwrap();
    let none = child(Ok::<Option<StoredObject>, GitError>(None));
    assert!(asks_child(&store.next(none)));
    assert_eq!(store.step, FileStep::Write);
}

fn writing(content: Content) -> WriteContentOperation {
    let mut store = filing();
    store.next(read_file()).unwrap();
    let sha256 = content.sha256();
    write_content(&store, content, sha256)
}

// LFS content held here is copied on the server.
#[test]
fn local_content_copied() {
    let mut write = writing(Content::Lfs(object(node(1))));
    assert!(asks_child(&write.first()));
    let none = child(Ok::<Option<StoredObject>, GitError>(None));
    assert!(asks_child(&write.next(none)));
    assert_eq!(write.step, WriteStep::Copied);
}

// LFS content held elsewhere streams from a holder through a put.
#[test]
fn remote_content_streamed() {
    let mut write = writing(Content::Lfs(object(node(2))));
    write.first().unwrap();
    let none = child(Ok::<Option<StoredObject>, GitError>(None));
    assert!(asks_child(&write.next(none)));
    assert_eq!(write.step, WriteStep::Holders);
    assert!(asks_child(
        &write.next(child(Ok::<_, GitError>(vec![node(2)])))
    ));
    assert_eq!(write.step, WriteStep::Open);
}

// A plain blob goes straight to the put.
#[test]
fn blob_put_directly() {
    let mut write = writing(Content::Blob(Bytes::from_static(b"text")));
    assert!(asks_child(&write.first()));
    assert_eq!(write.step, WriteStep::Put);
}

// A server-side copy is authorized as a read of the source.
#[test]
fn copy_needs_read() {
    let mut copy = CopiedOperation {
        routing: Default::default(),
        node: Some(node(1)),
        auth: auth(),
        source: object(node(1)),
        source_group: dataset().group_id,
        name: "bucket".to_string(),
        group_id: dataset().group_id,
        key: "key".to_string(),
        step: CopyStep::Authorize,
    };
    assert!(asks_child(&copy.first()));
    let refused = child(Err::<(), _>(AuthorizeError::PermissionDenied));
    let expected = GitError::Authorization(AuthorizeError::PermissionDenied);
    assert_eq!(copy.next(refused), Err(expected));
}
