//! Tests the watch check as calls: decisions without reads, the permission rules, the policy
//! sets and the two checks of a watch notice.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::notifications::user;
use aruna_core::calls::ChildOutcome;
use aruna_core::structs::identity::auth::{Actor, PathRestriction};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use ulid::Ulid;

fn owner() -> UserId {
    user(1, 2)
}

fn prefix() -> String {
    format!("meta/{}/", Ulid::from_parts(5, 5))
}

fn mask() -> WatchEventMask {
    WatchEventMask::from_kinds([WatchEventKind::MetadataCreated])
}

fn listing() -> WatchCheckOperation {
    let binding = WatchAuthorizationBinding::default();
    WatchCheckOperation::listing(owner().realm_id, owner(), &prefix(), mask(), &binding)
}

fn allowed(result: Result<bool, AuthorizationError>) -> IoResult {
    IoResult::Child(ChildOutcome::new(result))
}

/// A policy evaluator of a realm without request policies, which allows every request.
fn open_policies() -> PolicyEvaluator {
    let realm_id = owner().realm_id;
    let config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    let actor = Actor {
        node_id: crate::tests::locations::node_id(1),
        user_id: owner(),
        realm_id,
    };
    let bytes = config.to_bytes(&actor).unwrap();
    let mut load = PolicyLoadOperation::new(realm_id, None);
    load.first().unwrap();
    match load.next(IoResult::Values(vec![Some(bytes.into())])) {
        Ok(Next::Done(evaluator)) => evaluator,
        other => panic!("the realm policies load: {other:?}"),
    }
}

fn loaded(result: Result<PolicyEvaluator, PolicyEnforcementError>) -> IoResult {
    IoResult::Child(ChildOutcome::new(result))
}

fn done(authorization: WatchAuthorization) -> Turn {
    Ok(Next::Done(authorization))
}

fn denial(denial: WatchAuthorizationDenial) -> Turn {
    done(WatchAuthorization::Denied(denial))
}

#[test]
fn decided_without_reads() {
    let expired = WatchAuthorizationBinding {
        expires_at_secs: 0,
        ..Default::default()
    };
    let realm_id = owner().realm_id;
    let mut invalid = WatchCheckOperation::listing(realm_id, owner(), &prefix(), mask(), &expired);
    assert_eq!(
        invalid.first(),
        denial(WatchAuthorizationDenial::InvalidState)
    );

    let other = WatchAuthorizationBinding {
        watch_path_prefix: "meta/other/".to_string(),
        ..Default::default()
    };
    let mut moved = WatchCheckOperation::forwarded(realm_id, owner(), &prefix(), mask(), &other);
    assert_eq!(
        moved.first(),
        denial(WatchAuthorizationDenial::InvalidState)
    );

    let foreign = user(9, 2);
    let mut owner_check =
        WatchCheckOperation::listing(realm_id, foreign, &prefix(), mask(), &Default::default());
    assert_eq!(
        owner_check.first(),
        denial(WatchAuthorizationDenial::InvalidOwner)
    );

    let restricted = AuthContext {
        user_id: owner(),
        realm_id,
        path_restrictions: Some(vec![PathRestriction {
            pattern: "meta/**".to_string(),
            permission: Permission::READ,
        }]),
        session: None,
    };
    let mut token = WatchCheckOperation::creation(realm_id, &restricted, &prefix(), mask());
    assert_eq!(
        token.first(),
        denial(WatchAuthorizationDenial::TokenRestricted)
    );
}

/// The rules allow, then the realm policies allow: the watch is authorized.
#[test]
fn rules_then_policies() {
    let mut check = listing();
    assert!(matches!(check.first(), Ok(Next::Call(IoCall::Child(_)))));
    assert!(matches!(
        check.next(allowed(Ok(true))),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(
        check.next(loaded(Ok(open_policies()))),
        done(WatchAuthorization::Authorized)
    );
}

#[test]
fn refusals_keep_kind() {
    let mut denied_rules = listing();
    denied_rules.first().unwrap();
    assert_eq!(
        denied_rules.next(allowed(Ok(false))),
        denial(WatchAuthorizationDenial::PermissionDenied)
    );

    let mut missing = listing();
    missing.first().unwrap();
    assert!(matches!(
        missing.next(allowed(Err(AuthorizationError::DocNotFound))),
        Ok(Next::Done(WatchAuthorization::Unavailable(_)))
    ));

    let mut failed = listing();
    failed.first().unwrap();
    assert_eq!(
        failed.next(allowed(Err(AuthorizationError::NoTransactionFound))),
        Err(WatchCheckError::Authorization(
            AuthorizationError::NoTransactionFound
        ))
    );

    let mut policy = listing();
    policy.first().unwrap();
    policy.next(allowed(Ok(true))).unwrap();
    let refused = PolicyEnforcementError::Denied {
        name: "no-watch".to_string(),
        reason: "closed".to_string(),
    };
    assert_eq!(
        policy.next(loaded(Err(refused))),
        denial(WatchAuthorizationDenial::PermissionDenied)
    );

    let mut unread = listing();
    unread.first().unwrap();
    unread.next(allowed(Ok(true))).unwrap();
    let down = PolicyEnforcementError::Unavailable("down".to_string());
    assert_eq!(
        unread.next(loaded(Err(down))),
        done(WatchAuthorization::Unavailable("down".to_string()))
    );
}

/// A watch notice is checked against its watch scope, then the resource it names.
#[test]
fn notice_checks_twice() {
    let path = format!("{}dataset", prefix());
    let kind = NotificationKind::MetadataCreated {
        path,
        group_id: Ulid::from_parts(5, 5),
        document_id: Ulid::from_parts(6, 6),
        actor_user_id: owner(),
    };
    let binding = WatchAuthorizationBinding {
        watch_path_prefix: prefix(),
        ..Default::default()
    };
    let mut notice = WatchCheckOperation::notification(owner(), &kind, &binding);
    assert_eq!(notice.checks.len(), 2);
    notice.first().unwrap();
    notice.next(allowed(Ok(true))).unwrap();
    assert!(matches!(
        notice.next(loaded(Ok(open_policies()))),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(
        notice.next(allowed(Ok(false))),
        denial(WatchAuthorizationDenial::PermissionDenied)
    );

    let other = NotificationKind::AddedToGroup {
        group_id: Ulid::from_parts(5, 5),
        actor_user_id: owner(),
    };
    let mut unrelated = WatchCheckOperation::notification(owner(), &other, &binding);
    assert_eq!(
        unrelated.first(),
        denial(WatchAuthorizationDenial::InvalidState)
    );
}

#[test]
fn unexpected_results_fail() {
    let mut check = listing();
    check.first().unwrap();
    assert!(matches!(
        check.next(IoResult::Done),
        Err(WatchCheckError::Io(IoError::Unexpected { .. }))
    ));
}

fn watch(seed: u64) -> WatchSubscription {
    let mut watch = WatchSubscription::new(owner(), prefix(), mask(), 1_000);
    watch.watch_id = Ulid::from_parts(seed, seed);
    watch
}

fn verdict(result: Result<WatchAuthorization, WatchCheckError>) -> IoResult {
    IoResult::Child(ChildOutcome::new(result))
}

/// An owner's list keeps a denied watch with only its deletion id; an unavailable check fails.
#[test]
fn owner_list_redacts() {
    let mut list = WatchFilterOperation::owner(owner());
    assert!(matches!(list.first(), Ok(Next::Call(IoCall::Child(_)))));
    let page = WatchPage {
        subscriptions: vec![watch(1), watch(2)],
        next: None,
    };
    let listed = IoResult::Child(ChildOutcome::new(Ok::<_, WatchSubscriptionError>(page)));
    assert!(matches!(
        list.next(listed),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let authorized = verdict(Ok(WatchAuthorization::Authorized));
    assert!(matches!(
        list.next(authorized),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let denied = WatchAuthorization::Denied(WatchAuthorizationDenial::PermissionDenied);
    let Ok(Next::Done(outcome)) = list.next(verdict(Ok(denied))) else {
        panic!("both watches are checked");
    };
    assert_eq!(outcome.subscriptions[0], watch(1));
    let redacted = &outcome.subscriptions[1];
    assert_eq!(redacted.watch_id, watch(2).watch_id);
    assert!(redacted.path_prefix.is_empty() && redacted.event_mask == WatchEventMask::empty());

    let mut failing = WatchFilterOperation::owner(owner());
    failing.first().unwrap();
    let page = WatchPage {
        subscriptions: vec![watch(1)],
        next: None,
    };
    failing
        .next(IoResult::Child(ChildOutcome::new(Ok::<
            _,
            WatchSubscriptionError,
        >(page))))
        .unwrap();
    let down = WatchAuthorization::Unavailable("down".to_string());
    assert_eq!(
        failing.next(verdict(Ok(down))),
        Err(WatchCheckError::Unavailable("down".to_string()))
    );
}

/// A realm filter drops a watch held elsewhere, a denied watch and one whose check failed, and
/// reports the failed check.
#[test]
fn realm_filter_drops() {
    use aruna_core::structs::identity::realm::RealmNodeKind;
    let nodes = [1, 2].map(crate::tests::locations::node_id);
    let mut config = RealmConfigDocument::new(owner().realm_id, Vec::new(), 3);
    for node in nodes {
        config.ensure_node(node, RealmNodeKind::Server);
    }
    let holder = crate::notifications::placement::resolve_inbox_holder(&owner(), &config)
        .unwrap()
        .expect("one server holds the inbox");
    let elsewhere = nodes.into_iter().find(|node| *node != holder).unwrap();
    let stale = WatchFilterOperation::realm(owner().realm_id, &config, elsewhere, vec![watch(1)]);
    let mut stale = stale.unwrap();
    let dropped = AuthorizedWatchSubscriptions {
        subscriptions: Vec::new(),
        dropped: true,
        check_failed: false,
    };
    assert_eq!(stale.first(), Ok(Next::Done(dropped)));

    let watches = vec![watch(1), watch(2), watch(3)];
    let mut filter = WatchFilterOperation::realm(owner().realm_id, &config, holder, watches)
        .expect("the placement resolves");
    filter.first().unwrap();
    filter
        .next(verdict(Ok(WatchAuthorization::Authorized)))
        .unwrap();
    let denied = WatchAuthorization::Denied(WatchAuthorizationDenial::PermissionDenied);
    filter.next(verdict(Ok(denied))).unwrap();
    let failed = filter.next(verdict(Err(WatchCheckError::Io(IoError::Unfinished))));
    let Ok(Next::Done(outcome)) = failed else {
        panic!("every held watch is checked");
    };
    assert_eq!(outcome.subscriptions, vec![watch(1)]);
    assert!(outcome.dropped && outcome.check_failed);
}
