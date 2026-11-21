//! Tests finding the exact objects of a metadata graph call by call.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;

use aruna_core::calls::ChildOutcome;
use aruna_core::events::{Event, GroupEvent, GroupResult};
use ulid::Ulid;

use super::super::testing::{asks_child, auth, child, dataset, node, object, realm};
use super::*;

fn ids() -> NodeRealm {
    Some((node(1), realm()))
}

fn arn(holder: NodeId) -> VersionedObjectArn {
    VersionedObjectArn::new(realm(), holder, "bucket", "key", Ulid::from(3)).unwrap()
}

fn metadata(hashes: HashMap<String, Vec<u8>>) -> IoResult {
    child(Ok::<_, BaoReadError>(BaoReadOutput::Metadata {
        size: 4,
        blake3: [1; 32],
        etag: None,
        hashes,
    }))
}

fn global() -> DataIdentity {
    DataIdentity {
        global: Some(GlobalId::Version {
            realm_id: realm(),
            version_id: Ulid::from(3),
        }),
        ..DataIdentity::default()
    }
}

#[test]
fn exact_needs_node() {
    let mut read = resolve_exact(None, &arn(node(1)), &auth());
    assert_eq!(read.first(), Err(GitError::Unavailable));
}

// Another node's version is described by that node.
#[test]
fn remote_version_described() {
    let mut read = resolve_exact(Some(node(1)), &arn(node(2)), &auth());
    assert!(asks_child(&read.first()));
    let hashes = HashMap::from([
        (HASH_SHA256.to_string(), vec![0xab; 32]),
        (HASH_BLAKE3.to_string(), vec![1; 32]),
    ]);
    let Ok(Next::Done(found)) = read.next(metadata(hashes)) else {
        panic!("expected the object");
    };
    assert_eq!((found.node_id, found.group_id), (node(2), None));
    assert_eq!(found.sha256, "ab".repeat(32));
}

// A description without its digests is no usable answer.
#[test]
fn missing_digest_unavailable() {
    let mut read = resolve_exact(Some(node(1)), &arn(node(2)), &auth());
    read.first().unwrap();
    assert_eq!(
        read.next(metadata(HashMap::new())),
        Err(GitError::Unavailable)
    );
}

// A local read needs permission; a refusal looks like a missing object.
#[test]
fn refused_read_missing() {
    let mut read = resolve_exact(Some(node(1)), &arn(node(1)), &auth());
    read.step = ExactStep::Authorize;
    let refused = child(Err::<(), _>(AuthorizeError::PermissionDenied));
    assert_eq!(read.next(refused), Err(GitError::NotFound));
}

// A versioned ARN of this realm names the object without any read.
#[test]
fn exact_identity_direct() {
    let identity = DataIdentity {
        exact: Some(arn(node(2))),
        ..DataIdentity::default()
    };
    let mut find = located(ids(), &dataset(), &identity);
    assert_eq!(find.first(), Ok(Next::Done(vec![arn(node(2))])));
}

#[test]
fn empty_identity_nothing() {
    let mut find = located(ids(), &dataset(), &DataIdentity::default());
    assert_eq!(find.first(), Ok(Next::Done(Vec::new())));
}

// A global id this node keeps becomes a local exact version.
#[test]
fn global_found_here() {
    let mut find = located(ids(), &dataset(), &global());
    assert!(asks_child(&find.first()));
    let local = Some(("bucket".to_string(), "key".to_string()));
    let answer = find.next(child(Ok::<_, LocalVersionError>(local)));
    assert_eq!(answer, Ok(Next::Done(vec![arn(node(1))])));
}

// A global id this node cannot look up goes to the resolver under its budget.
#[test]
fn global_asks_resolver() {
    let mut find = located(ids(), &dataset(), &global());
    find.first().unwrap();
    let failed = LocalVersionError::Failed("no index".to_string());
    let answer = find.next(child(Err::<Option<(String, String)>, _>(failed)));
    assert!(matches!(answer, Ok(Next::Call(IoCall::Group(_)))));
}

// Candidates are tried in order and the last failure is reported.
#[test]
fn lookup_tries_candidates() {
    let mut find = lookup(ids(), &dataset(), &auth(), &DataIdentity::default());
    assert!(asks_child(&find.first()));
    let found = vec![arn(node(1)), arn(node(2))];
    assert!(asks_child(&find.next(child(Ok::<_, GitError>(found)))));
    let refused = child(Err::<StoredObject, _>(GitError::NotFound));
    assert!(asks_child(&find.next(refused)));
    let down = child(Err::<StoredObject, _>(GitError::Unavailable));
    assert_eq!(find.next(down), Err(GitError::Unavailable));
}

#[test]
fn lookup_without_candidates() {
    let mut find = lookup(ids(), &dataset(), &auth(), &DataIdentity::default());
    find.first().unwrap();
    let none = child(Ok::<_, GitError>(Vec::<VersionedObjectArn>::new()));
    assert_eq!(find.next(none), Err(GitError::NotFound));
}

// Text that is no JSON links nothing.
#[test]
fn invalid_graph_empty() {
    let mut links = linked(ids(), &dataset(), "not json", &auth());
    assert_eq!(links.first(), Ok(Next::Done(Vec::new())));
}

fn location_answer() -> IoResult {
    let document = dataset();
    let chosen = DatasetLocation::default_for(document.group_id, document.document_id);
    child(Ok::<_, GitError>((chosen, true)))
}

// A readable file is linked at its path; an unreadable one is left out.
#[test]
fn links_readable_files() {
    let graph = serde_json::json!({"@graph": [
        {"@id": "data/a.csv", "@type": "File"},
        {"@id": "data/b.csv", "@type": "File"},
        {"@id": "./", "@type": "Dataset"}]});
    let mut links = linked(ids(), &dataset(), &graph.to_string(), &auth());
    assert_eq!(links.files.len(), 2);
    links.first().unwrap();
    assert!(matches!(
        links.next(location_answer()),
        Ok(Next::Call(IoCall::Group(_)))
    ));
    let done = |outcome: Result<StoredObject, GitError>| {
        GroupResult::Done(Box::new(Event::Child(ChildOutcome::new(outcome))))
    };
    let results = vec![done(Ok(object(node(1)))), done(Err(GitError::NotFound))];
    let finished = IoResult::Group(GroupEvent::Finished { results });
    let Ok(Next::Done(found)) = links.next(finished) else {
        panic!("expected the links");
    };
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].entity, "data/a.csv");
    assert_eq!(found[0].path.as_deref(), Some("data/a.csv"));
}

#[test]
fn readable_entities_pass() {
    let mut check = unreadable_file(Some(node(1)), &auth(), &[]);
    assert_eq!(check.first(), Ok(Next::Done(None)));
}

// The first object that cannot be read names its entity.
#[test]
fn unreadable_entity_named() {
    let mut check = unreadable_file(Some(node(1)), &auth(), &[]);
    check.files.push_back(("entity".to_string(), arn(node(2))));
    assert!(asks_child(&check.first()));
    let refused = child(Err::<StoredObject, _>(GitError::NotFound));
    assert_eq!(
        check.next(refused),
        Ok(Next::Done(Some("entity".to_string())))
    );
}
