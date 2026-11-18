//! Tests redeeming download tickets.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::ChildOutcome;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::structs::storage::distribution::ticket::ticket_expiry;

use super::*;
use crate::document_repository::GetConfigError;

const NOW: u64 = 1_000;

fn secret(seed: u8) -> iroh::SecretKey {
    iroh::SecretKey::from_bytes(&[seed; 32])
}

fn realm() -> RealmId {
    RealmId::from_bytes([9; 32])
}

fn signed() -> SignedTicket {
    let grant = TokenGrant {
        token_hash: "hash".to_string(),
        issuer: realm(),
        issued_at_secs: NOW,
        expires_at_secs: NOW + 600,
        via: None,
    };
    let ticket = Ticket {
        realm_id: realm(),
        issuer: secret(1).public(),
        target: secret(2).public(),
        user_id: UserId::nil(realm()),
        expires_at: ticket_expiry(NOW, grant.expires_at_secs),
        grant,
        restrictions: None,
        session: None,
        object_id: "object".to_string(),
        nonce: [4; 16],
        issued_at: NOW,
    };
    let signature = secret(1).sign(&ticket.signing_bytes().unwrap());
    SignedTicket { signature, ticket }
}

fn redeem(ids: NodeRealm) -> RedeemTicketOperation {
    RedeemTicketOperation {
        ids,
        signed: signed(),
        object_id: "object".to_string(),
        now: NOW,
        step: RedeemStep::Config,
        ticket: None,
    }
}

fn target() -> NodeRealm {
    Some((secret(2).public(), realm()))
}

fn config(issuer_known: bool) -> RealmConfigDocument {
    let mut config = RealmConfigDocument::new(realm(), Vec::new(), 1);
    if issuer_known {
        config.ensure_node(secret(1).public(), RealmNodeKind::Server);
    }
    config
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn configured(known: bool) -> IoResult {
    child(Ok::<_, GetConfigError>(config(known)))
}

fn asks_child(next: &Redeeming) -> bool {
    matches!(next, Ok(Next::Call(IoCall::Child(_))))
}

/// A redemption whose issuer passed, waiting for the grant check.
fn checking() -> RedeemTicketOperation {
    let mut redeem = redeem(target());
    assert!(asks_child(&redeem.first()));
    assert!(asks_child(&redeem.next(configured(true))));
    redeem
}

// Without a transport, or for another node, nothing is read.
#[test]
fn refuses_before_reading() {
    assert_eq!(redeem(None).first(), Err(RedeemError::Unavailable));
    let elsewhere = Some((secret(3).public(), realm()));
    assert!(matches!(
        redeem(elsewhere).first(),
        Err(RedeemError::Ticket(_))
    ));
}

// Only a realm infrastructure node may issue tickets, and a missing config decides nothing.
#[test]
fn checks_issuer() {
    let mut unknown = redeem(target());
    unknown.first().unwrap();
    assert_eq!(unknown.next(configured(false)), Err(RedeemError::Issuer));
    let mut unread = redeem(target());
    unread.first().unwrap();
    let missing = child(Err::<RealmConfigDocument, _>(GetConfigError::DocumentNotFound));
    assert_eq!(unread.next(missing), Err(RedeemError::Unavailable));
}

// A revoked token ends the redemption; a failed check is no verdict about it.
#[test]
fn checks_grant() {
    let revoked = child(Ok::<_, ArunaBearerError>(false));
    assert_eq!(checking().next(revoked), Err(RedeemError::Revoked));
    let failed = child(Err::<bool, _>(ArunaBearerError::Expired));
    assert_eq!(checking().next(failed), Err(RedeemError::Unavailable));
}

// A ticket works once and then names its caller.
#[test]
fn spends_nonce_once() {
    let spend = |answer: Result<(), NonceError>| {
        let mut redeem = checking();
        assert!(asks_child(&redeem.next(child(Ok::<_, ArunaBearerError>(true)))));
        redeem.next(child(answer))
    };
    assert_eq!(spend(Err(NonceError::Replayed)), Err(RedeemError::Replayed));
    let Ok(Next::Done(auth)) = spend(Ok(())) else {
        panic!("expected the caller");
    };
    assert_eq!(auth.user_id, UserId::nil(realm()));
    assert_eq!(auth.realm_id, realm());
}
