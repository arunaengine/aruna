//! Tests the authorization operations as calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::calls::ChildOutcome;
use aruna_core::user::id::UserId;
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

fn admin_group() -> GroupId {
    Ulid::from_bytes([3; 16])
}

/// The group admin path, and `alternative` in the same realm.
fn either_with(alternative: &str) -> AuthorizeEitherOperation {
    let path = format!("/{}/g/{}/admin", auth().realm_id, admin_group());
    let paths = (path.as_str(), alternative);
    AuthorizeEitherOperation::new(auth().realm_id, &auth(), paths, &Permission::WRITE)
}

/// The group admin path, or else the realm groups administration path, as the route asks.
fn either() -> AuthorizeEitherOperation {
    either_with(&format!("/{}/admin/groups", auth().realm_id))
}

/// The realm and the group authorization rows, the caller holding WRITE on `granted`.
fn rule_rows(granted: &str) -> IoResult {
    use aruna_core::structs::identity::auth::{Actor, Role};
    use aruna_core::structs::identity::group::GroupAuthorizationDocument;
    use aruna_core::structs::identity::realm::RealmAuthorizationDocument;
    use std::collections::HashSet;
    let realm_id = auth().realm_id;
    let actor = Actor {
        node_id: iroh::SecretKey::from_bytes(&[1; 32]).public(),
        user_id: auth().user_id,
        realm_id,
    };
    let role = Role {
        role_id: Ulid::from_bytes([9; 16]),
        name: "admins".to_string(),
        permissions: HashMap::from([(granted.to_string(), Permission::WRITE)]),
        assigned_users: HashSet::from([auth().user_id]),
    };
    let realm = RealmAuthorizationDocument {
        realm_id,
        roles: HashMap::from([(role.role_id, role)]),
        operation_restrictions: HashMap::new(),
    };
    let group = GroupAuthorizationDocument {
        group_id: admin_group(),
        roles: HashMap::new(),
        policies: Vec::new(),
    };
    IoResult::Values(vec![
        Some(realm.to_bytes(&actor).unwrap().into()),
        Some(group.to_bytes(&actor).unwrap().into()),
    ])
}

/// A policy evaluator of a realm without request policies, which allows every request.
fn open_policies() -> IoResult {
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::realm::RealmConfigDocument;
    let realm_id = auth().realm_id;
    let actor = Actor {
        node_id: iroh::SecretKey::from_bytes(&[1; 32]).public(),
        user_id: auth().user_id,
        realm_id,
    };
    let bytes = RealmConfigDocument::new(realm_id, Vec::new(), 3).to_bytes(&actor);
    let mut load = PolicyLoadOperation::new(realm_id, None);
    load.first().unwrap();
    let Ok(Next::Call(compile)) = load.next(IoResult::Values(vec![Some(bytes.unwrap().into())]))
    else {
        panic!("the realm policies compile");
    };
    let Ok(Next::Done(evaluator)) = load.next(crate::tests::calls::compiled(compile)) else {
        panic!("the realm policies load");
    };
    let answer: Result<PolicyEvaluator, PolicyEnforcementError> = Ok(evaluator);
    IoResult::Child(ChildOutcome::new(answer))
}

#[test]
fn either_reads_once() {
    // The realm administration path needs no row the group admin path does not read.
    let Ok(Next::Call(IoCall::Read(reads))) = either().first() else {
        panic!("both paths are decided from one read");
    };
    assert_eq!(reads.len(), 2);
}

#[test]
fn either_direct_grant() {
    // A direct grant enforces only the policies of its own path.
    let mut operation = either();
    operation.first().unwrap();
    let granted = format!("/{}/g/{}/admin", auth().realm_id, admin_group());
    assert!(is_child(&operation.next(rule_rows(&granted))));
    assert_eq!(operation.next(open_policies()), Ok(Next::Done(())));
}

#[test]
fn either_alternative_grant() {
    // A grant on the alternative enforces both paths' policies, each group loaded on its own.
    let mut operation = either();
    operation.first().unwrap();
    let granted = format!("/{}/admin/groups", auth().realm_id);
    assert!(is_child(&operation.next(rule_rows(&granted))));
    assert!(is_child(&operation.next(open_policies())));
    assert_eq!(operation.next(open_policies()), Ok(Next::Done(())));
}

#[test]
fn either_group_shared() {
    // Both paths of one group are enforced after one policy load.
    let other = format!("/{}/g/{}/other", auth().realm_id, admin_group());
    let mut operation = either_with(&other);
    operation.first().unwrap();
    assert!(is_child(&operation.next(rule_rows(&other))));
    assert_eq!(operation.next(open_policies()), Ok(Next::Done(())));
}

#[test]
fn either_neither_denied() {
    let mut operation = either();
    operation.first().unwrap();
    let elsewhere = format!("/{}/other", auth().realm_id);
    assert_eq!(
        operation.next(rule_rows(&elsewhere)),
        Err(AuthorizeError::PermissionDenied)
    );
}

#[test]
fn either_policy_final() {
    let mut operation = either();
    operation.first().unwrap();
    operation
        .next(rule_rows(&format!("/{}/admin/groups", auth().realm_id)))
        .unwrap();
    let denied: Result<PolicyEvaluator, PolicyEnforcementError> =
        Err(PolicyEnforcementError::Denied {
            name: "rule".to_string(),
            reason: "no".to_string(),
        });
    let answer = operation.next(IoResult::Child(ChildOutcome::new(denied)));
    assert!(matches!(answer, Err(AuthorizeError::Policy(_))));
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
