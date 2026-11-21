//! Drives the identifier lookup through its index pages and document checks without I/O.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::types::{Key, Value};

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn query() -> IdentifierQuery {
    IdentifierQuery {
        kind: SecondaryIdKind::Doi,
        value: "10.1234/1".to_string(),
        endpoint: None,
    }
}

fn index_row(document: u8) -> (Key, Value) {
    let mut key = secondary_id_prefix(SecondaryIdKind::Doi, "10.1234/1", None);
    key.extend_from_slice(&[document; 16]);
    (Key::from(key), Value::from(Vec::new()))
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn check(document: u8) -> DocumentMatchOperation {
    DocumentMatchOperation {
        realm_id: realm(),
        auth: None,
        query: query(),
        document_id: Ulid::from_bytes([document; 16]),
        stage: MatchStage::Record,
    }
}

#[test]
fn empty_index_unmatched() {
    let mut lookup = IdentifierLookupOperation::new(realm(), None, query());
    assert!(matches!(
        lookup.first(),
        Ok(Next::Call(IoCall::Page { .. }))
    ));
    let empty = IoResult::Page {
        rows: Vec::new(),
        next: None,
    };
    assert_eq!(lookup.next(empty), Ok(Next::Done(Vec::new())));
}

#[test]
fn documents_checked_once() {
    let mut lookup = IdentifierLookupOperation::new(realm(), None, query());
    lookup.first().unwrap();
    let page = IoResult::Page {
        rows: vec![index_row(2), index_row(1), index_row(2)],
        next: None,
    };
    let Ok(Next::Call(IoCall::Group(group))) = lookup.next(page) else {
        panic!("expected the document checks");
    };
    assert_eq!(group.members.len(), 2);
    let found = IdentifierMatch {
        document_id: Ulid::from_bytes([1; 16]),
        pid: None,
        origin: aruna_core::structs::secondary_id::IdentifierOrigin::Published,
    };
    let member = |result: Result<Option<IdentifierMatch>, MetadataApiError>| {
        GroupResult::Done(Box::new(Event::Child(ChildOutcome::new(result))))
    };
    let results = vec![member(Ok(Some(found.clone()))), member(Ok(None))];
    let finished = IoResult::Group(GroupEvent::Finished { results });
    assert_eq!(lookup.next(finished), Ok(Next::Done(vec![found])));
}

#[test]
fn gone_document_skipped() {
    let mut check = check(1);
    assert!(matches!(check.first(), Ok(Next::Call(IoCall::Child(_)))));
    let gone = child(Ok::<Option<MetadataRegistryRecord>, StorageReadError>(None));
    assert_eq!(check.next(gone), Ok(Next::Done(None)));
}

#[test]
fn unreadable_document_skipped() {
    let mut check = check(1);
    check.stage = MatchStage::Readable;
    let denied = child(Err::<(), _>(MetadataApiError::NotFound));
    assert_eq!(check.next(denied), Ok(Next::Done(None)));
}

#[test]
fn unmapped_document_skipped() {
    let mut check = check(1);
    check.stage = MatchStage::Mapping;
    let unmapped = child(Ok::<Option<PersistentIdMapping>, PersistentIdError>(None));
    assert_eq!(check.next(unmapped), Ok(Next::Done(None)));
}
