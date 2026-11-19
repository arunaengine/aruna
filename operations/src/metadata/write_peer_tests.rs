//! Drives the forwarded caller check through its realm, token, cutoff and config answers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::calls::{bucket_realm, child, node, runs_child};
use crate::tests::records::REALM;
use aruna_core::types::Value;
use ulid::Ulid;

fn realm_answer(realm: Option<RealmId>) -> IoResult {
    IoResult::Event(Box::new(Event::Net(NetEvent::LocalRealm(realm))))
}

// The realm config with `peer` as a server node, as storage holds it.
fn config_row(peer: NodeId) -> IoResult {
    let config = bucket_realm(REALM, Ulid::from_parts(1, 1), &[peer]);
    let bytes = postcard::to_allocvec(&config).unwrap();
    IoResult::Values(vec![Some(Value::from(bytes))])
}

fn internal() -> AuthToken {
    AuthToken::Internal(AuthContext::anonymous(REALM))
}

fn contact(peer: NodeId) -> Turn {
    Ok(Next::Call(net_call(NetEffect::PeerContact(peer))))
}

#[test]
fn vouched_peer_accepted() {
    let mut check = WritePeerOperation::new(node(1), internal());
    assert_eq!(
        check.first(),
        Ok(Next::Call(net_call(NetEffect::LocalRealm)))
    );
    let read = vec![(
        REALM_CONFIG_KEYSPACE.to_string(),
        Key::from(REALM.as_bytes().to_vec()),
    )];
    let config = check.next(realm_answer(Some(REALM)));
    assert_eq!(config, Ok(Next::Call(IoCall::Read(read))));
    assert_eq!(check.next(config_row(node(1))), contact(node(1)));
    let recorded = IoResult::Event(Box::new(Event::Net(NetEvent::PeerContact)));
    let done = check.next(recorded);
    assert_eq!(done, Ok(Next::Done(AuthContext::anonymous(REALM))));
}

#[test]
fn quiet_skips_contact() {
    let mut check = WritePeerOperation::quiet(node(1), internal());
    check.first().unwrap();
    check.next(realm_answer(Some(REALM))).unwrap();
    let done = check.next(config_row(node(1)));
    assert_eq!(done, Ok(Next::Done(AuthContext::anonymous(REALM))));
}

#[test]
fn unlisted_peer_refused() {
    let mut check = WritePeerOperation::new(node(2), internal());
    check.first().unwrap();
    check.next(realm_answer(Some(REALM))).unwrap();
    let refused = check.next(config_row(node(1)));
    assert!(
        matches!(refused, Err(PeerAuthRefusal::Unavailable(message)) if message.contains("not configured"))
    );
}

#[test]
fn foreign_internal_refused() {
    let mut check = WritePeerOperation::new(node(1), internal());
    check.first().unwrap();
    let other = RealmId::from_bytes([9; 32]);
    let refused = check.next(realm_answer(Some(other)));
    assert!(matches!(refused, Err(PeerAuthRefusal::Unavailable(_))));
}

#[test]
fn stale_replay_refused() {
    let replay = AuthToken::Replay {
        auth: AuthContext::anonymous(REALM),
        issued_at: 10,
        authored_at_ms: 20_000,
    };
    let mut check = WritePeerOperation::new(node(1), replay);
    check.first().unwrap();
    assert!(runs_child(check.next(realm_answer(Some(REALM)))));
    let cutoff = child(Ok::<_, ArunaBearerError>(Some(11)));
    assert_eq!(check.next(cutoff), Err(PeerAuthRefusal::Unauthorized));
}

#[test]
fn replay_age_authoring() {
    // A replay is as old as the earlier of its token issue time and its authoring time.
    let replay = |issued_at, authored_at_ms| AuthToken::Replay {
        auth: AuthContext::anonymous(REALM),
        issued_at,
        authored_at_ms,
    };
    for (token, refused) in [(replay(100, 50_000), true), (replay(100, 105_000), false)] {
        let mut check = WritePeerOperation::new(node(1), token);
        check.first().unwrap();
        check.next(realm_answer(Some(REALM))).unwrap();
        let answered = check.next(child(Ok::<_, ArunaBearerError>(Some(100))));
        assert_eq!(answered == Err(PeerAuthRefusal::Unauthorized), refused);
    }
}

#[test]
fn bad_bearer_refused() {
    let bearer = AuthToken::bearer("token").unwrap();
    let mut check = WritePeerOperation::new(node(1), bearer);
    check.first().unwrap();
    assert!(runs_child(check.next(realm_answer(Some(REALM)))));
    let refused = child(Err::<aruna_core::structs::identity::auth::TokenClaims, _>(
        ArunaBearerError::TokenRevoked,
    ));
    assert_eq!(check.next(refused), Err(PeerAuthRefusal::Unauthorized));
}

#[test]
fn checks_follow_realm() {
    assert_eq!(
        forwarded_checks(None, false).revocation,
        RevocationCheck::Skipped
    );
    assert_eq!(
        forwarded_checks(Some(REALM), false).revocation,
        RevocationCheck::Realm
    );
    assert_eq!(
        forwarded_checks(Some(REALM), true).revocation,
        RevocationCheck::Skipped
    );
}

#[test]
fn mirror_refuses_replay() {
    // A sync mirror never takes a device replay as its caller.
    let replay = AuthToken::Replay {
        auth: AuthContext::anonymous(REALM),
        issued_at: 10,
        authored_at_ms: 20_000,
    };
    let mut check = WritePeerOperation::mirror(node(1), replay);
    check.first().unwrap();
    assert_eq!(
        check.next(realm_answer(Some(REALM))),
        Err(PeerAuthRefusal::Unauthorized)
    );
}

#[test]
fn mirror_checks_foreign_bearer() {
    // A bearer of another realm still needs the peer in that realm's config.
    let foreign = RealmId([7u8; 32]);
    let mut check = WritePeerOperation::mirror(node(1), AuthToken::bearer("token").unwrap());
    check.first().unwrap();
    assert!(runs_child(check.next(realm_answer(Some(REALM)))));
    let claims = aruna_core::structs::identity::auth::TokenClaims {
        sub: aruna_core::UserId::local(Ulid::from_bytes([1u8; 16]), foreign).to_string(),
        iss: foreign.to_base64(),
        iat: 1,
        exp: 2,
        jti: "token-id".to_string(),
        sid: None,
        session_kind: None,
        restrictions: None,
        issuer_pubkey: None,
        delegation_signature: None,
        name: None,
        via: None,
        auth_time: None,
    };
    let claims = child(Ok::<_, ArunaBearerError>(claims));
    let read = vec![(
        REALM_CONFIG_KEYSPACE.to_string(),
        Key::from(foreign.as_bytes().to_vec()),
    )];
    assert_eq!(check.next(claims), Ok(Next::Call(IoCall::Read(read))));
}
