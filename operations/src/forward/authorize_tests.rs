//! Tests the checks of forwarded callers as calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::UserId;
use aruna_core::calls::ChildOutcome;
use aruna_core::structs::identity::realm::RealmNodeKind;

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn auth(realm_id: RealmId) -> AuthContext {
    AuthContext {
        user_id: UserId::local(Ulid::from_bytes([2; 16]), realm_id),
        realm_id,
        path_restrictions: None,
        session: None,
    }
}

fn checked(result: Result<AuthContext, WritePeerRefusal>) -> IoResult {
    let event = NetEvent::WritePeerChecked(result.map(Box::new));
    IoResult::Event(Box::new(Event::Net(event)))
}

fn config_child(config: RealmConfigDocument) -> IoResult {
    IoResult::Child(ChildOutcome::new(Ok::<_, GetConfigError>(config)))
}

fn message() -> MetadataTransportMessage {
    MetadataTransportMessage::ForwardDeleteDocument {
        auth_token: None,
        config_digest: [0; 32],
        document_id: Ulid::from_bytes([3; 16]),
    }
}

fn caller() -> ForwardedCallerOperation {
    let mut operation = ForwardedCallerOperation::new(node(1), realm(), &message());
    assert!(matches!(operation.first(), Ok(Next::Call(IoCall::Effect(_)))));
    operation
}

#[test]
fn refusals_keep_kind() {
    let mut denied = caller();
    assert_eq!(
        denied.next(checked(Err(WritePeerRefusal::Unauthorized))),
        Err(ForwardAuthError::Unauthorized)
    );
    let mut down = caller();
    let refusal = WritePeerRefusal::Unavailable("down".to_string());
    assert_eq!(
        down.next(checked(Err(refusal))),
        Err(ForwardAuthError::Unavailable("down".to_string()))
    );
}

#[test]
fn foreign_realm_forbidden() {
    let mut operation = caller();
    let foreign = auth(RealmId::from_bytes([9; 32]));
    assert_eq!(
        operation.next(checked(Ok(foreign))),
        Err(ForwardAuthError::Forbidden)
    );
}

#[test]
fn server_peer_acts() {
    let mut config = RealmConfigDocument::new(realm(), Vec::new(), 2);
    config.ensure_node(node(1), RealmNodeKind::Server);
    let mut operation = caller();
    let turn = operation.next(checked(Ok(auth(realm()))));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(
        operation.next(config_child(config)),
        Ok(Next::Done(auth(realm())))
    );
}

#[test]
fn device_bound_owner() {
    // A device may forward only its own owner's writes.
    let other = UserId::local(Ulid::from_bytes([8; 16]), realm());
    let mut config = RealmConfigDocument::new(realm(), Vec::new(), 2);
    config.ensure_node(node(1), RealmNodeKind::User { owner: other });
    let mut operation = caller();
    operation.next(checked(Ok(auth(realm())))).unwrap();
    assert_eq!(
        operation.next(config_child(config)),
        Err(ForwardAuthError::Forbidden)
    );
}

#[test]
fn plain_token_skips_owner() {
    let mut operation = ForwardedCallerOperation::for_token(node(1), realm(), None);
    operation.first().unwrap();
    assert_eq!(
        operation.next(checked(Ok(auth(realm())))),
        Ok(Next::Done(auth(realm())))
    );
}

fn create(scope: CreateScope) -> AuthorizeCreateOperation {
    AuthorizeCreateOperation::new(
        realm(),
        &auth(realm()),
        PolicyRequestExtras::rest(),
        Ulid::from_bytes([4; 16]),
        "docs/a",
        Ulid::from_bytes([5; 16]),
        scope,
    )
}

fn passed() -> IoResult {
    IoResult::Child(ChildOutcome::new(Ok::<(), AuthorizeError>(())))
}

#[test]
fn group_scope_checks_twice() {
    let mut operation = create(CreateScope::Group);
    assert!(matches!(operation.first(), Ok(Next::Call(IoCall::Child(_)))));
    assert!(matches!(
        operation.next(passed()),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(operation.next(passed()), Ok(Next::Done(())));
}

#[test]
fn document_scope_checks_once() {
    let mut operation = create(CreateScope::Document);
    operation.first().unwrap();
    assert_eq!(operation.next(passed()), Ok(Next::Done(())));
}

#[test]
fn denied_create_stops() {
    let mut operation = create(CreateScope::Group);
    operation.first().unwrap();
    let denied = Err::<(), _>(AuthorizeError::PermissionDenied);
    assert_eq!(
        operation.next(IoResult::Child(ChildOutcome::new(denied))),
        Err(AuthorizeError::PermissionDenied)
    );
}
