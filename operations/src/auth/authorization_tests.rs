//! Tests the authorization operations as calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::UserId;
use aruna_core::calls::ChildOutcome;
use ulid::Ulid;

fn auth() -> AuthContext {
    let realm_id = RealmId([1; 32]);
    AuthContext {
        user_id: UserId::local(Ulid::from_bytes([2; 16]), realm_id),
        realm_id,
        path_restrictions: None,
        session: None,
    }
}

fn checked(allowed: bool) -> IoResult {
    let answer: Result<bool, AuthorizationError> = Ok(allowed);
    IoResult::Child(ChildOutcome::new(answer))
}

fn policy_passed() -> IoResult {
    let answer: Result<(), PolicyEnforcementError> = Ok(());
    IoResult::Child(ChildOutcome::new(answer))
}

fn policy_denied() -> IoResult {
    let answer: Result<(), PolicyEnforcementError> = Err(PolicyEnforcementError::Denied {
        name: "rule".to_string(),
        reason: "no".to_string(),
    });
    IoResult::Child(ChildOutcome::new(answer))
}

fn authorize() -> AuthorizeOperation {
    let extras = PolicyRequestExtras::rest();
    AuthorizeOperation::new(auth().realm_id, &auth(), "/r", &Permission::READ, extras)
}

fn is_child<T>(turn: &Result<Next<T>, AuthorizeError>) -> bool {
    matches!(turn, Ok(Next::Call(IoCall::Child(_))))
}

#[test]
fn checks_then_enforces() {
    let mut operation = authorize();
    assert!(is_child(&operation.first()));
    assert!(is_child(&operation.next(checked(true))));
    assert_eq!(operation.next(policy_passed()), Ok(Next::Done(())));
}

#[test]
fn denied_skips_policies() {
    let mut operation = authorize();
    operation.first().unwrap();
    assert_eq!(
        operation.next(checked(false)),
        Err(AuthorizeError::PermissionDenied)
    );
}

#[test]
fn policy_denial_final() {
    let mut operation = authorize();
    operation.first().unwrap();
    operation.next(checked(true)).unwrap();
    assert!(matches!(
        operation.next(policy_denied()),
        Err(AuthorizeError::Policy(_))
    ));
}

#[test]
fn missing_target_denied() {
    // A target that cannot be resolved is a denial, not a fault.
    let mut operation = authorize();
    operation.first().unwrap();
    let answer: Result<bool, AuthorizationError> = Err(AuthorizationError::GroupNotFound);
    assert_eq!(
        operation.next(IoResult::Child(ChildOutcome::new(answer))),
        Err(AuthorizeError::PermissionDenied)
    );
}

fn either() -> AuthorizeEitherOperation {
    let paths = ("/a", "/b");
    AuthorizeEitherOperation::new(auth().realm_id, &auth(), paths, &Permission::WRITE)
}

#[test]
fn either_direct_grant() {
    // A direct grant enforces only the policies of its own path.
    let mut operation = either();
    operation.first().unwrap();
    assert!(is_child(&operation.next(checked(true))));
    assert_eq!(operation.next(policy_passed()), Ok(Next::Done(())));
}

#[test]
fn either_alternative_grant() {
    // A grant on the alternative enforces both paths' policies.
    let mut operation = either();
    operation.first().unwrap();
    assert!(is_child(&operation.next(checked(false))));
    assert!(is_child(&operation.next(checked(true))));
    assert!(is_child(&operation.next(policy_passed())));
    assert_eq!(operation.next(policy_passed()), Ok(Next::Done(())));
}

#[test]
fn either_neither_denied() {
    let mut operation = either();
    operation.first().unwrap();
    operation.next(checked(false)).unwrap();
    assert_eq!(
        operation.next(checked(false)),
        Err(AuthorizeError::PermissionDenied)
    );
}

#[test]
fn public_skips_roles() {
    let extras = PolicyRequestExtras::rest();
    let realm_id = auth().realm_id;
    let public = AuthorizeDocumentOperation::new(realm_id, &auth(), ("/d", true), extras.clone());
    assert!(matches!(public, AuthorizeDocumentOperation::Public(_)));
    let private = AuthorizeDocumentOperation::new(realm_id, &auth(), ("/d", false), extras);
    assert!(matches!(private, AuthorizeDocumentOperation::Private(_)));
}

#[test]
fn anonymous_rules_empty() {
    let group = Ulid::from_bytes([3; 16]);
    let mut operation = GroupRulesOperation::new(None, [group]);
    assert_eq!(
        operation.first(),
        Ok(Next::Done(GroupPermissionRules::default()))
    );
}

#[test]
fn failed_group_hidden() {
    // A group whose rules cannot be collected keeps none; the others still count.
    let (first, second) = (Ulid::from_bytes([3; 16]), Ulid::from_bytes([4; 16]));
    let auth = auth();
    let mut operation = GroupRulesOperation::new(Some(&auth), [second, first, first]);
    assert!(matches!(
        operation.first(),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let failed: Result<PermissionRules, AuthorizationError> =
        Err(AuthorizationError::GroupNotFound);
    let turn = operation.next(IoResult::Child(ChildOutcome::new(failed)));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let rules: Result<PermissionRules, AuthorizationError> = Ok(PermissionRules::default());
    let Ok(Next::Done(_)) = operation.next(IoResult::Child(ChildOutcome::new(rules))) else {
        panic!("expected the collected rules");
    };
    assert_eq!(operation.groups.len(), 0);
}
