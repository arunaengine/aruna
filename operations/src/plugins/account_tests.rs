//! Tests the account and role decisions of an install as calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::ChildOutcome;
use ulid::Ulid;

use super::*;
use crate::plugins::rows::tests::values_result;
use crate::plugins::work::tests::sample_install;

fn realm() -> RealmId {
    RealmId::from_bytes([1u8; 32])
}

fn actor(install: &PluginInstall) -> Actor {
    Actor {
        node_id: install.receiver,
        user_id: install.granted_by,
        realm_id: realm(),
    }
}

fn change() -> (AccountChange, PluginInstall) {
    let install = sample_install(Ulid::from_parts(1, 1));
    let actor = actor(&install);
    let auth = AuthContext::from(&actor);
    (AccountChange::of((&actor, &auth), &install, 9), install)
}

fn role_of(change: &AccountChange, users: &[UserId], name: &str) -> Role {
    Role {
        role_id: change.identity.role_id,
        name: name.to_string(),
        permissions: HashMap::new(),
        assigned_users: users.iter().copied().collect(),
    }
}

fn group_doc(change: &AccountChange, roles: Vec<Role>) -> Vec<u8> {
    let scope = change.scope;
    let user = change.actor.user_id;
    let mut doc =
        GroupAuthorizationDocument::default_group_doc(user, scope.realm_id, scope.group_id);
    doc.roles = roles.into_iter().map(|role| (role.role_id, role)).collect();
    doc.to_bytes(&change.actor).unwrap()
}

fn stepped_ok() -> IoResult {
    IoResult::Child(ChildOutcome::new(Ok::<(), String>(())))
}

fn step_failed(why: &str) -> IoResult {
    IoResult::Child(ChildOutcome::new(Err::<(), String>(why.to_string())))
}

fn is_child<T>(next: Next<T>) -> bool {
    matches!(next, Next::Call(IoCall::Child(_)))
}

#[test]
fn creates_missing_account() {
    let (change, _) = change();
    let mut ensure = EnsureAccount::new(change, "name".into());
    assert!(matches!(ensure.first(), Ok(Next::Call(IoCall::Read(rows))) if rows.len() == 3));
    let next = ensure.next(values_result(vec![None, None, None])).unwrap();
    assert!(is_child(next), "a missing account is created");
    assert_eq!(ensure.next(stepped_ok()), Ok(Next::Done(())));
}

#[test]
fn refuses_shared_account() {
    let (change, _) = change();
    let account = change.identity.account;
    let mut other = role_of(&change, &[account], READ_ROLE);
    other.role_id = Ulid::from_parts(7, 7);
    let row = group_doc(&change, vec![other]);
    let mut ensure = EnsureAccount::new(change, "name".into());
    let refused = ensure.next(values_result(vec![None, Some(row), None]));
    let expected = AccountError("the account holds another role of the group".into());
    assert_eq!(refused, Err(expected));
}

#[test]
fn write_failure_named() {
    let (change, _) = change();
    let mut ensure = EnsureAccount::new(change, "name".into());
    ensure.next(values_result(vec![None, None, None])).unwrap();
    let failed = ensure.next(step_failed("refused"));
    assert_eq!(failed, Err(AccountError("refused".into())));
}

#[test]
fn role_needs_group() {
    let (change, _) = change();
    let mut missing = EnsureRole::new(change.clone(), HashMap::new());
    let refused = missing.next(values_result(vec![None]));
    assert_eq!(refused, Err(AccountError("the group is missing".into())));
    // A role under this id that is not the install's own is never replaced.
    let foreign = group_doc(&change, vec![role_of(&change, &[], "admin")]);
    let mut taken = EnsureRole::new(change.clone(), HashMap::new());
    let refused = taken.next(values_result(vec![Some(foreign)]));
    let expected = AccountError("the role id belongs to another role".into());
    assert_eq!(refused, Err(expected));
    let own = role_of(&change, &[change.identity.account], READ_ROLE);
    let own = group_doc(&change, vec![own]);
    let mut set = EnsureRole::new(change, HashMap::new());
    assert!(is_child(set.next(values_result(vec![Some(own)])).unwrap()));
    assert_eq!(set.next(stepped_ok()), Ok(Next::Done(())));
}

#[test]
fn ready_needs_account() {
    let (change, _) = change();
    let mut check = CheckReady::new(change.scope, change.identity, HashMap::new());
    assert!(matches!(check.first(), Ok(Next::Call(IoCall::Read(rows))) if rows.len() == 5));
    let refused = check.next(values_result(vec![None; 5]));
    assert_eq!(refused, Err(AccountError("the account is missing".into())));
}

#[test]
fn teardown_skips_absent() {
    let (change, _) = change();
    let mut teardown = Teardown::new(change.clone());
    teardown.first().unwrap();
    let role_read = teardown.next(values_result(vec![None])).unwrap();
    let group = vec![group_auth_row(change.scope.group_id)];
    assert_eq!(role_read, Next::Call(read_call(group)));
    assert_eq!(teardown.next(values_result(vec![None])), Ok(Next::Done(())));
}

#[test]
fn teardown_removes_role() {
    let (change, _) = change();
    let own = group_doc(&change, vec![role_of(&change, &[], WRITE_ROLE)]);
    let mut teardown = Teardown::new(change);
    teardown.next(values_result(vec![None])).unwrap();
    let removal = teardown.next(values_result(vec![Some(own)]));
    assert!(is_child(removal.unwrap()));
    assert_eq!(teardown.next(stepped_ok()), Ok(Next::Done(())));
}

#[test]
fn name_falls_back() {
    let group_id = Ulid::from_parts(4, 4);
    let mut name = AccountName::new("copier", group_id);
    name.first().unwrap();
    let answer = name.next(values_result(vec![None]));
    assert_eq!(
        answer,
        Ok(Next::Done(format!("plugin copier ({group_id})")))
    );
}

#[test]
fn make_ready_order() {
    let (change, install) = change();
    let mut ready = MakeReady::new(change, &install);
    assert!(is_child(ready.first().unwrap()));
    let named = IoResult::Child(ChildOutcome::new(Ok::<_, AccountError>("n".to_string())));
    assert!(is_child(ready.next(named).unwrap()), "the account step");
    let done = || IoResult::Child(ChildOutcome::new(Ok::<(), AccountError>(())));
    assert!(is_child(ready.next(done()).unwrap()), "the role step");
    assert!(is_child(ready.next(done()).unwrap()), "the check");
    let checked = Ok::<Vec<Prerequisite>, AccountError>(Vec::new());
    let answer = ready.next(IoResult::Child(ChildOutcome::new(checked)));
    assert_eq!(answer, Ok(Next::Done(Vec::new())));
}

#[test]
fn ready_names_step() {
    let (change, install) = change();
    let mut ready = MakeReady::new(change, &install);
    let unnamed = Err::<String, _>(AccountError("gone".into()));
    assert!(is_child(
        ready
            .next(IoResult::Child(ChildOutcome::new(unnamed)))
            .unwrap()
    ));
    let failed = Err::<(), _>(AccountError("no".into()));
    let answer = ready.next(IoResult::Child(ChildOutcome::new(failed)));
    let expected = StepError {
        step: "account",
        error: "no".into(),
    };
    assert_eq!(answer, Err(expected));
}

#[test]
fn repair_stops_early() {
    let (change, install) = change();
    let mut repair = RepairAccount::new(change, &install);
    assert!(is_child(repair.first().unwrap()));
    let ready = Ok::<Vec<Prerequisite>, AccountError>(Vec::new());
    let answer = repair.next(IoResult::Child(ChildOutcome::new(ready)));
    assert_eq!(answer, Ok(Next::Done(false)));
}

#[test]
fn repair_provisions_missing() {
    let (change, install) = change();
    let mut repair = RepairAccount::new(change, &install);
    let unready = Err::<Vec<Prerequisite>, _>(AccountError("no".into()));
    let read = repair.next(IoResult::Child(ChildOutcome::new(unready)));
    assert!(matches!(read, Ok(Next::Call(IoCall::Read(_)))));
    assert!(is_child(repair.next(values_result(vec![None])).unwrap()));
    assert_eq!(repair.phase, RepairPhase::Provisioning);
}

#[test]
fn realm_admin_smallest() {
    let (first, second) = (
        UserId::local(Ulid::from_parts(1, 1), realm()),
        UserId::local(Ulid::from_parts(2, 2), realm()),
    );
    let mut document = RealmAuthorizationDocument::default_realm_doc(realm());
    for role in document.roles.values_mut() {
        if role.name == REALM_ADMIN_ROLE {
            role.assigned_users = [second, first].into();
        }
    }
    let actor = Actor {
        node_id: iroh::SecretKey::from_bytes(&[1; 32]).public(),
        user_id: first,
        realm_id: realm(),
    };
    let row = document.to_bytes(&actor).unwrap();
    let mut admin = RealmAdmin(realm());
    admin.first().unwrap();
    assert_eq!(
        admin.next(values_result(vec![Some(row)])),
        Ok(Next::Done(Some(first)))
    );
}

#[test]
fn no_prerequisites_skip() {
    let mut install = sample_install(Ulid::from_parts(1, 1));
    install.prerequisites.clear();
    assert_eq!(
        PrerequisitesHeld::new(&install).first(),
        Ok(Next::Done(true))
    );
    let target = AdminDocumentTarget::Group {
        group_id: install.group_id,
    };
    install.prerequisites.push(Prerequisite {
        target,
        clock: Default::default(),
    });
    let mut held = PrerequisitesHeld::new(&install);
    assert!(matches!(held.first(), Ok(Next::Call(IoCall::Read(_)))));
    assert_eq!(held.next(values_result(vec![None])), Ok(Next::Done(false)));
}

#[test]
fn absent_not_plugin() {
    let mut account = PluginAccount(UserId::local(Ulid::from_parts(3, 3), realm()));
    account.first().unwrap();
    assert_eq!(
        account.next(values_result(vec![None])),
        Ok(Next::Done(false))
    );
}
