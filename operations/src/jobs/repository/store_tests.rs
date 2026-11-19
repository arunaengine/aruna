//! Checks the calls and results of the link reads and of one applied link change.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::jobs::repository::test_support::{
    NOW_MS, child, link, link_bytes, local_scope, other_node, scope,
};
use aruna_core::repository::{LinkDirection, LinkPatch};

fn pages() -> IoCall {
    PrefixPages::new(
        REPOSITORY_LINK_KEYSPACE,
        ByteView::from(link_prefix(link().document_id)),
        LINK_PAGE,
    )
    .call()
}

fn page(rows: Vec<RepositoryLink>, next: Option<Key>) -> IoResult {
    IoResult::Page {
        rows: rows
            .iter()
            .map(|link| (ByteView::from(vec![1]), link_bytes(link)))
            .collect(),
        next,
    }
}

#[test]
fn read_parses_row() {
    let stored = link();
    let mut operation = ReadLinkOperation::new(stored.document_id, stored.link_id);
    let row = link_row(stored.document_id, stored.link_id);
    assert_eq!(operation.first(), Ok(Next::Call(IoCall::Read(vec![row]))));
    let answer = IoResult::Values(vec![Some(link_bytes(&stored))]);
    assert_eq!(operation.next(answer), Ok(Next::Done(Some(stored))));
}

#[test]
fn links_follow_pages() {
    let mut operation = DocumentLinksOperation::new(link().document_id);
    assert_eq!(operation.first(), Ok(Next::Call(pages())));
    let more = operation.next(page(vec![link()], Some(ByteView::from(vec![1]))));
    assert!(matches!(
        more,
        Ok(Next::Call(IoCall::Page { after: Some(_), .. }))
    ));
    let done = operation.next(page(vec![link()], None));
    assert_eq!(done, Ok(Next::Done(vec![link(), link()])));
}

#[test]
fn list_marks_queued() {
    let stored = link();
    let mut operation = ListLinksOperation::new(stored.document_id);
    assert_eq!(
        operation.first(),
        Ok(Next::Call(IoCall::child(DocumentLinksOperation::new(
            stored.document_id
        ))))
    );
    let read = operation
        .next(child::<_, LinkError>(Ok(vec![stored.clone()])))
        .unwrap();
    let row = (LINK_QUEUE_KEYSPACE.to_string(), id_key(stored.link_id));
    assert_eq!(read, Next::Call(IoCall::Read(vec![row])));
    let answer = IoResult::Values(vec![Some(ByteView::from(vec![1]))]);
    assert_eq!(operation.next(answer), Ok(Next::Done(vec![(stored, true)])));
}

#[test]
fn list_without_links() {
    let mut operation = ListLinksOperation::new(link().document_id);
    operation.first().unwrap();
    let answer = child::<Vec<RepositoryLink>, LinkError>(Ok(Vec::new()));
    assert_eq!(operation.next(answer), Ok(Next::Done(Vec::new())));
}

#[test]
fn lineage_refuses_opposite() {
    let mut pushing = link();
    pushing.remote.parent_id = Some("6".into());
    let mut pulling = pushing.clone();
    pulling.link_id = Ulid::from_bytes([9; 16]);
    pulling.direction = LinkDirection::Pull(Box::new(pull_state()));
    let mut operation = EnsureLineageOperation::new(pushing);
    operation.first().unwrap();
    let answer = child::<_, LinkError>(Ok(vec![pulling]));
    assert_eq!(operation.next(answer), Err(LinkError::Lineage));
}

fn pull_state() -> aruna_core::repository::LinkPull {
    aruna_core::repository::LinkPull {
        auto_update: false,
        options: Default::default(),
        target: aruna_core::structs::execution::job::ImportRoCrateTarget {
            bucket: "b".into(),
            prefix: String::new(),
        },
        latest_remote_id: None,
        latest_revision: None,
        last_checked_at: None,
        next_check_ms: 0,
        failures: 0,
        revision: None,
        local_changed: false,
    }
}

#[test]
fn foreign_owner_refused() {
    let mut scope = scope();
    scope.node = scope.node.map(|(_, realm)| (other_node(), realm));
    let mut operation = ApplyChangeOperation::new(scope, link(), LinkChange::Delete);
    assert_eq!(
        operation.first(),
        Err(LinkError::NotOwner(link().owner_node_url))
    );
}

#[test]
fn local_change_writes() {
    let stored = link();
    let mut operation =
        ApplyChangeOperation::new(local_scope(), stored.clone(), LinkChange::Delete);
    operation.stamp(NOW_MS);
    let expected = ChangeLinkOperation::new(
        stored.document_id,
        stored.link_id,
        LinkChange::Delete,
        time_of(NOW_MS),
    );
    assert_eq!(operation.first(), Ok(Next::Call(IoCall::child(expected))));
    let answer = child::<Option<RepositoryLink>, LinkError>(Ok(None));
    assert_eq!(operation.next(answer), Ok(Next::Done(None)));
}

#[test]
fn resume_checks_lineage() {
    let stored = link();
    let resume = LinkChange::Patch(LinkPatch {
        paused: Some(false),
        ..LinkPatch::default()
    });
    let mut operation = ApplyChangeOperation::new(local_scope(), stored.clone(), resume);
    assert_eq!(
        operation.first(),
        Ok(Next::Call(IoCall::child(EnsureLineageOperation::new(
            stored
        ))))
    );
    let refused = operation.next(child::<(), LinkError>(Err(LinkError::Lineage)));
    assert_eq!(refused, Err(LinkError::Lineage));
}

#[test]
fn missing_config_fails() {
    let mut operation = ApplyChangeOperation::new(scope(), link(), LinkChange::Delete);
    assert_eq!(
        operation.first(),
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(
            crate::jobs::repository::test_support::realm()
        ))))
    );
    let answer =
        child::<RealmConfigDocument, GetConfigError>(Err(GetConfigError::DocumentNotFound));
    assert!(matches!(
        operation.next(answer),
        Err(LinkError::Unexpected(_))
    ));
}
