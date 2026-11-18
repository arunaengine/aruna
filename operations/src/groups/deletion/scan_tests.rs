//! Tests what a deletion check asks and answers, as calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::document_repository::GetConfigError;
use crate::tests::calls::{caller_of, child, node, owned_group, runs_child};
use aruna_core::structs::identity::group::Group;
use aruna_core::structs::identity::realm::RealmNodeKind;
use ulid::Ulid;

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn group_id() -> GroupId {
    Ulid::from_bytes([3; 16])
}

fn config(kind: RealmNodeKind) -> RealmConfigDocument {
    let mut config = RealmConfigDocument::new(realm(), Vec::new(), 2);
    config.ensure_node(node(1), kind);
    config.ensure_node(node(2), RealmNodeKind::Server);
    config
}

fn device() -> RealmNodeKind {
    RealmNodeKind::User {
        owner: auth().user_id,
    }
}

fn clear() -> IoResult {
    child(Ok::<(), GroupDeletionError>(()))
}

fn conflict() -> GroupDeletionError {
    GroupDeletionError::Conflict("busy".to_string())
}

fn refusal(buckets: &[&str]) -> GroupDeletionError {
    GroupDeletionError::NotEmpty {
        kinds: vec!["S3 buckets".to_string()],
        buckets: buckets.iter().map(|name| name.to_string()).collect(),
        more: true,
        hidden: 1,
    }
}

fn loaded(group: Option<Group>) -> IoResult {
    let answer: LoadedGroup = (group, None);
    child(Ok::<_, GroupDeletionError>(answer))
}

fn scanning() -> ScanOperation {
    let mut operation = ScanOperation::new(&auth(), group_id(), node(1));
    operation.stamp(1_000);
    assert!(runs_child(operation.first()));
    operation
}

#[test]
fn empty_group_passes() {
    assert_eq!(scanning().next(clear()), Ok(Next::Done(())));
}

#[test]
fn scan_error_kept() {
    let failed = child(Err::<(), _>(conflict()));
    assert_eq!(scanning().next(failed), Err(conflict()));
}

#[test]
fn hidden_buckets_counted() {
    let mut operation = scanning();
    let blocked = child(Err::<(), _>(refusal(&["shown", "hidden"])));
    assert!(runs_child(operation.next(blocked)));
    let names = vec!["shown".to_string()];
    let visible = child(Ok::<_, GroupDeletionError>(names.clone()));
    assert_eq!(
        operation.next(visible),
        Err(GroupDeletionError::NotEmpty {
            kinds: vec!["S3 buckets".to_string()],
            buckets: names,
            more: true,
            hidden: 2,
        })
    );
}

fn node_check(kind: RealmNodeKind) -> CheckNodeOperation {
    let config = config(kind);
    let mut operation = CheckNodeOperation::new(&config, &auth(), group_id(), node(1));
    assert!(runs_child(operation.first()));
    operation
}

#[test]
fn holder_authorizes_first() {
    let mut operation = node_check(RealmNodeKind::Server);
    assert!(runs_child(operation.next(loaded(Some(group())))));
    assert_eq!(operation.stage, NodeStage::Authorize);
    assert!(runs_child(operation.next(clear())));
    assert_eq!(operation.stage, NodeStage::Scan);
    assert_eq!(operation.next(clear()), Ok(Next::Done(())));
}

#[test]
fn refused_caller_stops() {
    let mut operation = node_check(RealmNodeKind::Server);
    operation.next(loaded(Some(group()))).unwrap();
    let refused = child(Err::<(), _>(GroupDeletionError::Unauthorized));
    assert_eq!(
        operation.next(refused),
        Err(GroupDeletionError::Unauthorized)
    );
}

#[test]
fn unsynced_server_unavailable() {
    assert!(matches!(
        node_check(RealmNodeKind::Server).next(loaded(None)),
        Err(GroupDeletionError::Unavailable(_))
    ));
}

#[test]
fn device_scans_directly() {
    let mut operation = node_check(device());
    assert!(runs_child(operation.next(loaded(None))));
    assert_eq!(operation.stage, NodeStage::Scan);
}

fn local() -> Option<(NodeId, RealmId)> {
    Some((node(1), realm()))
}

fn group_check(kind: RealmNodeKind, group: Option<Group>) -> CheckGroupOperation {
    let mut operation = CheckGroupOperation::new(local(), auth(), group_id());
    assert!(runs_child(operation.first()));
    let read = child(Ok::<_, GetConfigError>(config(kind)));
    assert!(runs_child(operation.next(read)));
    assert!(runs_child(operation.next(loaded(group))));
    assert!(matches!(operation.stage, GroupStage::Authorize { .. }));
    operation
}

#[test]
fn check_needs_network() {
    let mut operation = CheckGroupOperation::new(None, auth(), group_id());
    assert!(matches!(
        operation.first(),
        Err(GroupDeletionError::Unavailable(_))
    ));
}

#[test]
fn foreign_caller_refused() {
    let other = Some((node(1), RealmId::from_bytes([9; 32])));
    let mut operation = CheckGroupOperation::new(other, auth(), group_id());
    assert_eq!(operation.first(), Err(GroupDeletionError::Unauthorized));
}

#[test]
fn unknown_group_missing() {
    assert_eq!(
        group_check(RealmNodeKind::Server, None).next(clear()),
        Err(GroupDeletionError::NotFound)
    );
}

#[test]
fn device_cannot_check() {
    assert!(matches!(
        group_check(device(), Some(group())).next(clear()),
        Err(GroupDeletionError::Unavailable(_))
    ));
}

#[test]
fn blockers_are_sorted() {
    let mut operation = group_check(RealmNodeKind::Server, Some(group()));
    assert!(runs_child(operation.next(clear())));
    assert_eq!(operation.stage, GroupStage::Scan);
    let blocked = child(Err::<(), _>(conflict()));
    assert!(runs_child(operation.next(blocked)));
    assert_eq!(operation.stage, GroupStage::Ask);
    let replies: NodeReplies = vec![(node(2), Ok(None))];
    let mut expected = vec![(node(1), Err(conflict())), (node(2), Ok(()))];
    expected.sort_by_key(|(node, _)| *node);
    assert_eq!(
        operation.next(child(Ok::<_, GroupDeletionError>(replies))),
        Ok(Next::Done(expected))
    );
}

#[test]
fn node_errors_kept() {
    let mut operation = group_check(RealmNodeKind::Server, Some(group()));
    operation.next(clear()).unwrap();
    operation.next(clear()).unwrap();
    let replies: NodeReplies = vec![(node(2), Err(conflict()))];
    let Ok(Next::Done(blockers)) = operation.next(child(Ok::<_, GroupDeletionError>(replies)))
    else {
        panic!("expected the blockers");
    };
    assert!(blockers.contains(&(node(2), Err(conflict()))));
    assert!(blockers.contains(&(node(1), Ok(()))));
}

fn auth() -> AuthContext {
    caller_of(realm())
}

fn group() -> Group {
    owned_group(group_id(), &auth())
}
