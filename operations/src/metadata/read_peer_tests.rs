//! Drives the forwarded read and search caller checks through their answers without I/O.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::calls::{child, node, runs_child};
use aruna_core::structs::identity::realm::RealmNodeKind;

fn realm() -> RealmId {
    RealmId::from_bytes([4; 32])
}

fn bearer() -> Option<AuthToken> {
    AuthToken::bearer("token").ok()
}

fn caller(realm_id: RealmId) -> IoResult {
    child(Ok::<_, PeerAuthRefusal>(AuthContext::anonymous(realm_id)))
}

// Node 1 is a configured server of the realm; node 2 is not configured.
fn config() -> IoResult {
    let mut config = RealmConfigDocument::new(realm(), Vec::new(), 2);
    config.ensure_node(node(1), RealmNodeKind::Server);
    child(Ok::<_, GetConfigError>(config))
}

fn contact_call() -> Result<Next<Option<AuthContext>>, MetadataReadError> {
    let contact = Effect::Net(NetEffect::PeerContact(node(1)));
    Ok(Next::Call(IoCall::Effect(Box::new(contact))))
}

fn contacted() -> IoResult {
    IoResult::Event(Box::new(aruna_core::events::Event::Net(
        aruna_core::events::NetEvent::PeerContact,
    )))
}

// The bearer check refreshes the last contact itself, so the read adds no refresh.
#[test]
fn bearer_read_checked() {
    let mut read = ReadPeerOperation::new((node(1), realm()), bearer(), false);
    let check = WritePeerOperation::new(node(1), bearer().unwrap());
    assert_eq!(read.first(), Ok(Next::Call(IoCall::child(check))));
    let auth = AuthContext::anonymous(realm());
    assert_eq!(read.next(caller(realm())), Ok(Next::Done(Some(auth))));
}

#[test]
fn foreign_caller_forbidden() {
    let mut read = ReadPeerOperation::new((node(1), realm()), bearer(), false);
    read.first().unwrap();
    let foreign = RealmId::from_bytes([5; 32]);
    assert_eq!(
        read.next(caller(foreign)),
        Err(MetadataReadError::Forbidden)
    );
}

#[test]
fn refused_token_unauthorized() {
    let mut read = ReadPeerOperation::new((node(1), realm()), bearer(), false);
    read.first().unwrap();
    let refused = child(Err::<AuthContext, _>(PeerAuthRefusal::Unauthorized));
    assert_eq!(read.next(refused), Err(MetadataReadError::Unauthorized));
}

#[test]
fn replay_read_refused() {
    let token = AuthToken::Replay {
        auth: AuthContext::anonymous(realm()),
        issued_at: 1,
        authored_at_ms: 1,
    };
    let mut read = ReadPeerOperation::new((node(1), realm()), Some(token), false);
    assert_eq!(read.first(), Err(MetadataReadError::Unavailable));
}

#[test]
fn anonymous_member_reads() {
    let mut read = ReadPeerOperation::new((node(1), realm()), None, true);
    assert!(runs_child(read.first()));
    assert_eq!(read.next(config()), contact_call());
    assert_eq!(read.next(contacted()), Ok(Next::Done(None)));
}

// An internal token is checked quietly; the accepted read refreshes the contact afterwards.
#[test]
fn internal_read_contacts() {
    let token = AuthToken::Internal(AuthContext::anonymous(realm()));
    let mut read = ReadPeerOperation::new((node(1), realm()), Some(token.clone()), false);
    let check = WritePeerOperation::quiet(node(1), token);
    assert_eq!(read.first(), Ok(Next::Call(IoCall::child(check))));
    assert_eq!(read.next(caller(realm())), contact_call());
    let auth = AuthContext::anonymous(realm());
    assert_eq!(read.next(contacted()), Ok(Next::Done(Some(auth))));
}

#[test]
fn unknown_peer_unavailable() {
    let mut read = ReadPeerOperation::new((node(2), realm()), None, false);
    read.first().unwrap();
    assert_eq!(read.next(config()), Err(MetadataReadError::Unavailable));
}

#[test]
fn trusted_read_vouches() {
    let mut read = ReadPeerOperation::new((node(2), realm()), bearer(), true);
    read.first().unwrap();
    assert!(runs_child(read.next(caller(realm()))));
    assert_eq!(read.next(config()), Err(MetadataReadError::Unavailable));
}

#[test]
fn object_search_needs_bearer() {
    let mut search = SearchPeerOperation::new((node(1), realm()), None, PeerReader::Objects);
    assert_eq!(search.first(), Err(MetadataReadError::Unauthorized));
}

#[test]
fn bucket_search_anonymous() {
    let mut search = SearchPeerOperation::new((node(1), realm()), None, PeerReader::Buckets);
    assert!(runs_child(search.first()));
    let anonymous = AuthContext::anonymous(realm());
    assert_eq!(search.next(config()), Ok(Next::Done(anonymous)));
}

// A search caller's check leaves the peer's last contact as it was.
#[test]
fn bearer_search_checked() {
    let reader = PeerReader::Objects;
    let mut search = SearchPeerOperation::new((node(1), realm()), bearer(), reader);
    let check = WritePeerOperation::quiet(node(1), bearer().unwrap());
    assert_eq!(search.first(), Ok(Next::Call(IoCall::child(check))));
    let auth = AuthContext::anonymous(realm());
    assert_eq!(search.next(caller(realm())), Ok(Next::Done(auth)));
}

#[test]
fn release_needs_vouching() {
    let reader = PeerReader::Release;
    let mut search = SearchPeerOperation::new((node(2), realm()), bearer(), reader);
    search.first().unwrap();
    assert!(runs_child(search.next(caller(realm()))));
    assert_eq!(search.next(config()), Err(MetadataReadError::Forbidden));
}
