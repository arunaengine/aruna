//! Tests the deletion access decisions as calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::UserId;
use aruna_core::calls::ChildOutcome;
use aruna_core::structs::identity::group_delete::GroupDeletePlan;
use aruna_core::structs::identity::realm::RealmId;
use std::collections::BTreeSet;
use ulid::Ulid;

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn node() -> NodeId {
    iroh::SecretKey::from_bytes(&[2; 32]).public()
}

fn group() -> GroupId {
    Ulid::from_bytes([3; 16])
}

fn auth() -> AuthContext {
    AuthContext {
        user_id: UserId::local(Ulid::from_bytes([4; 16]), realm()),
        realm_id: realm(),
        path_restrictions: None,
        session: None,
    }
}

fn allowed() -> IoResult {
    IoResult::Child(ChildOutcome::new(Ok::<(), AuthorizeError>(())))
}

fn denied() -> IoResult {
    let denied = Err::<(), _>(AuthorizeError::PermissionDenied);
    IoResult::Child(ChildOutcome::new(denied))
}

fn deleted_record(requested_by: UserId) -> GroupDeleteRecord {
    GroupDeleteRecord {
        plan: GroupDeletePlan {
            request_id: Ulid::from_bytes([5; 16]),
            group_id: group(),
            realm_id: realm(),
            owner: UserId::nil(realm()),
            requested_by,
            coordinator: node(),
            nodes: BTreeSet::new(),
        },
        phase: GroupDeletePhase::Deleted,
        deleted_by: None,
        event: None,
    }
}

#[test]
fn restricted_caller_refused() {
    let restricted = AuthContext {
        path_restrictions: Some(Vec::new()),
        ..auth()
    };
    let mut operation = AuthorizeDeleteOperation::new(&restricted, group(), None, true);
    assert_eq!(operation.first(), Err(GroupDeletionError::Unauthorized));
}

#[test]
fn requester_sees_deletion() {
    let record = deleted_record(auth().user_id);
    let mut operation = AuthorizeDeleteOperation::new(&auth(), group(), Some(&record), false);
    assert_eq!(operation.first(), Ok(Next::Done(())));
}

#[test]
fn group_admin_checked() {
    let mut operation = AuthorizeDeleteOperation::new(&auth(), group(), None, true);
    assert!(matches!(
        operation.first(),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert!(matches!(
        operation.next(denied()),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(operation.next(allowed()), Ok(Next::Done(())));
}

#[test]
fn every_path_denied() {
    let mut operation = AuthorizeDeleteOperation::new(&auth(), group(), None, false);
    operation.first().unwrap();
    assert_eq!(
        operation.next(denied()),
        Err(GroupDeletionError::Unauthorized)
    );
}

#[test]
fn visible_drops_denied() {
    let names = vec!["shown".to_string(), "hidden".to_string()];
    let mut operation = VisibleOperation::new(&auth(), group(), node(), names);
    operation.first().unwrap();
    operation.next(allowed()).unwrap();
    assert_eq!(
        operation.next(denied()),
        Ok(Next::Done(vec!["shown".to_string()]))
    );
}

#[test]
fn other_refusal_unchanged() {
    let error = GroupDeletionError::Conflict("retry".to_string());
    let mut operation = BlockedOperation::new(&auth(), group(), node(), error.clone());
    assert_eq!(operation.first(), Ok(Next::Done(error)));
}

#[test]
fn absent_rows_empty() {
    let mut operation = LoadGroupOperation::new(group());
    assert!(matches!(operation.first(), Ok(Next::Call(IoCall::Read(_)))));
    assert_eq!(
        operation.next(IoResult::Values(vec![None, None])),
        Ok(Next::Done((None, None)))
    );
}

#[test]
fn garbled_group_unavailable() {
    let mut operation = LoadGroupOperation::new(group());
    operation.first().unwrap();
    let garbled = aruna_core::types::Value::from(vec![0xff]);
    assert!(matches!(
        operation.next(IoResult::Values(vec![Some(garbled), None])),
        Err(GroupDeletionError::Unavailable(_))
    ));
}
